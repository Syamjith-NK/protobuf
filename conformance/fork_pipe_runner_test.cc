// Protocol Buffers - Google's data interchange format
// Copyright 2025 Google LLC.  All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd

// Hermetic tests for ForkPipeRunner's process management.  The testee is
// /bin/sh running small scripts that stand in for a well-behaved, a stubborn,
// a crashing, a signal-killed and a hung conformance testee.  The pipe protocol
// is a 4-byte little-endian length prefix followed by the payload in each
// direction, so `cat` is a testee that echoes every request back verbatim, and
// a 3-byte request is exactly 7 bytes on the wire.

// TODO: b/418427266 - add Windows coverage; these tests drive the testee with
// /bin/sh and inspect process groups.
#ifndef _WIN32

#include "fork_pipe_runner.h"

#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>  // NOLINT(build/c++11)
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "conformance/conformance.pb.h"

namespace google {
namespace protobuf {

// Test-only access to the runner's timeouts, so that tests of the SIGKILL
// fallback and of the read-timeout path do not have to wait out the production
// defaults.
class ForkPipeRunnerPeer {
 public:
  static void SetShutdownGracePeriod(ForkPipeRunner& runner,
                                     std::chrono::milliseconds grace_period) {
    runner.shutdown_grace_period_ = grace_period;
  }
  static void SetReadTimeout(ForkPipeRunner& runner,
                             std::chrono::milliseconds read_timeout) {
    runner.read_timeout_ = read_timeout;
  }
};

namespace {

using ::testing::HasSubstr;

constexpr char kShell[] = "/bin/sh";

// True iff this process has no child processes left, live or zombie.  The
// test process has no children other than the ones ForkPipeRunner spawns.
bool NoChildRemains() {
  return waitpid(-1, nullptr, WNOHANG) == -1 && errno == ECHILD;
}

std::unique_ptr<ForkPipeRunner> MakeRunner(const std::string& script) {
  const std::vector<std::string> args = {"-c", script};
  return std::make_unique<ForkPipeRunner>(kShell, args);
}

conformance::ConformanceResponse ParseResponse(const std::string& serialized) {
  conformance::ConformanceResponse response;
  EXPECT_TRUE(response.ParseFromString(serialized));
  return response;
}

// A pipe that nobody writes to, whose write end is inherited by the testee and
// by everything the testee forks.  Its read end therefore reports EOF exactly
// when every process holding the write end has died, which detects a leaked
// grandchild without depending on who reaps it or on pid reuse.
class InheritedPipe {
 public:
  InheritedPipe() { EXPECT_EQ(pipe(fds_), 0) << strerror(errno); }
  ~InheritedPipe() {
    CloseWriteEnd();
    close(fds_[0]);
  }

  // To be called once the testee has been spawned, so that this process no
  // longer holds the write end open itself.
  void CloseWriteEnd() {
    if (fds_[1] >= 0) {
      close(fds_[1]);
      fds_[1] = -1;
    }
  }

  // True iff every process holding the write end has died within `timeout`.
  bool WritersAreGone(std::chrono::milliseconds timeout) const {
    pollfd pfd = {};
    pfd.fd = fds_[0];
    pfd.events = POLLIN;
    if (poll(&pfd, 1, static_cast<int>(timeout.count())) <= 0) return false;
    char c;
    return read(fds_[0], &c, 1) == 0;
  }

 private:
  int fds_[2] = {-1, -1};
};

TEST(ForkPipeRunnerTest, CooperativeTesteeIsReaped) {
  // `cat` echoes the length-prefixed request straight back, and exits on EOF
  // like a real testee does.
  auto runner = MakeRunner("exec cat");
  EXPECT_EQ(runner->RunTest("t", "abc"), "abc");

  const auto start = std::chrono::steady_clock::now();
  runner.reset();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  // A cooperative testee must not be made to wait for the grace period.
  EXPECT_LT(elapsed, std::chrono::seconds(5));
  EXPECT_TRUE(NoChildRemains());
}

TEST(ForkPipeRunnerTest, PipesAreNotInheritedByAnotherTestee) {
  // The second testee is forked while the first one's pipes are open.  Were
  // it to inherit the write end of the first testee's stdin, the first `cat`
  // would not see EOF when its runner shuts down and would have to be killed
  // after the grace period.
  auto first = MakeRunner("exec cat");
  constexpr std::chrono::milliseconds kGrace(2000);
  ForkPipeRunnerPeer::SetShutdownGracePeriod(*first, kGrace);
  EXPECT_EQ(first->RunTest("t", "abc"), "abc");
  auto second = MakeRunner("exec cat");
  EXPECT_EQ(second->RunTest("t", "def"), "def");

  const auto start = std::chrono::steady_clock::now();
  first.reset();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(elapsed, kGrace);
  EXPECT_EQ(second->RunTest("t", "ghi"), "ghi");
  second.reset();
  EXPECT_TRUE(NoChildRemains());
}

TEST(ForkPipeRunnerTest, StubbornTesteeIsKilled) {
  // Answers one request, then ignores EOF on stdin and lives forever.
  auto runner = MakeRunner("trap '' PIPE; cat; while :; do sleep 1; done");
  constexpr std::chrono::milliseconds kGrace(100);
  ForkPipeRunnerPeer::SetShutdownGracePeriod(*runner, kGrace);
  EXPECT_EQ(runner->RunTest("t", "abc"), "abc");

  const auto start = std::chrono::steady_clock::now();
  runner.reset();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  // The destructor waits out the grace period, then SIGKILLs the testee and
  // reaps it promptly.
  EXPECT_GE(elapsed, kGrace);
  EXPECT_LT(elapsed, kGrace + std::chrono::seconds(5));
  EXPECT_TRUE(NoChildRemains());
}

TEST(ForkPipeRunnerTest, StubbornTesteesGrandchildIsKilledToo) {
  InheritedPipe pipe;
  // Forks a grandchild that lives forever, answers one request, then ignores
  // EOF itself.  The grandchild is a wrapper script's real testee stand-in;
  // it is in the testee's process group but is not our child.
  auto runner = MakeRunner("sleep 1000 & cat; exec sleep 1000");
  constexpr std::chrono::milliseconds kGrace(100);
  ForkPipeRunnerPeer::SetShutdownGracePeriod(*runner, kGrace);
  EXPECT_EQ(runner->RunTest("t", "abc"), "abc");
  pipe.CloseWriteEnd();
  ASSERT_FALSE(pipe.WritersAreGone(std::chrono::milliseconds(0)))
      << "the testee should be holding the pipe open";

  runner.reset();

  // Killing only the direct child would leave `sleep 1000 &` running (and
  // holding the pipe's write end), so EOF here proves the whole process group
  // was killed.
  EXPECT_TRUE(pipe.WritersAreGone(std::chrono::seconds(5)))
      << "grandchild outlived Shutdown()";
  EXPECT_TRUE(NoChildRemains());
}

TEST(ForkPipeRunnerTest, CrashedTesteeIsReapedAndRespawned) {
  // Consumes the 7-byte request, then exits with status 3 without answering.
  auto runner = MakeRunner("head -c 7 >/dev/null; exit 3");

  conformance::ConformanceResponse response =
      ParseResponse(runner->RunTest("t", "abc"));
  EXPECT_THAT(response.runtime_error(), HasSubstr("status=3"));
  EXPECT_FALSE(response.has_timeout_error());
  EXPECT_TRUE(NoChildRemains()) << "crash path must reap the testee";

  // The next call respawns the (same) testee, which fails the same way.
  response = ParseResponse(runner->RunTest("t2", "abc"));
  EXPECT_THAT(response.runtime_error(), HasSubstr("status=3"));

  runner.reset();
  EXPECT_TRUE(NoChildRemains());
}

TEST(ForkPipeRunnerTest, SignaledTesteeIsReported) {
  // Consumes the request (so the runner's write cannot race the death of the
  // reader and raise SIGPIPE here), then kills itself.
  auto runner = MakeRunner("head -c 7 >/dev/null; kill -9 $$");

  conformance::ConformanceResponse response =
      ParseResponse(runner->RunTest("t", "abc"));
  EXPECT_THAT(response.runtime_error(), HasSubstr("killed by signal 9"));
  EXPECT_FALSE(response.has_timeout_error());

  runner.reset();
  EXPECT_TRUE(NoChildRemains());
}

TEST(ForkPipeRunnerTest, HungTesteeTimesOutAndIsKilled) {
  // Consumes the request, then hangs without answering.  It ignores SIGQUIT
  // (the ignore disposition survives the exec) and prints nothing in response
  // to it, which is the case in which the diagnostic read after the timeout
  // must be bounded, and it holds its stdout open so that read never sees EOF.
  auto runner =
      MakeRunner("trap '' QUIT; head -c 7 >/dev/null; exec sleep 1000");
  constexpr std::chrono::milliseconds kReadTimeout(200);
  ForkPipeRunnerPeer::SetReadTimeout(*runner, kReadTimeout);

  const auto start = std::chrono::steady_clock::now();
  conformance::ConformanceResponse response =
      ParseResponse(runner->RunTest("t", "abc"));
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_THAT(response.timeout_error(), HasSubstr("child timed out"));
  EXPECT_THAT(response.timeout_error(), HasSubstr("killed by runner"));
  EXPECT_FALSE(response.has_runtime_error());
  EXPECT_GE(elapsed, kReadTimeout);
  // Read timeout, plus the bounded SIGQUIT diagnostic read (1 s idle), plus
  // the 2 s grace before SIGKILL; anything much longer means something
  // blocked.
  EXPECT_LT(elapsed, std::chrono::seconds(15));
  EXPECT_TRUE(NoChildRemains()) << "timeout path must reap the testee";

  runner.reset();
  EXPECT_TRUE(NoChildRemains());
}

}  // namespace
}  // namespace protobuf
}  // namespace google

#endif  // !_WIN32
