// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd

#include "fork_pipe_runner.h"

#ifndef _WIN32

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>  // NOLINT(build/c++11)
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <string>
#include <thread>  // NOLINT(build/c++11)
#include <vector>

#include "absl/log/absl_log.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define CHECK_SYSCALL(call)                              \
  do {                                                   \
    if (call < 0) {                                      \
      perror(#call " " __FILE__ ":" TOSTRING(__LINE__)); \
      exit(1);                                           \
    }                                                    \
  } while (0)
// For use between fork() and execv() in the child of a multi-threaded process,
// where only async-signal-safe calls are allowed: no perror() (it uses stdio
// and may take locks held by another thread in the parent) and no exit() (it
// runs atexit handlers and flushes stdio), just a fixed message and _exit().
#define CHECK_SYSCALL_IN_CHILD(call)                                          \
  do {                                                                        \
    if (call < 0) {                                                           \
      static constexpr char kMessage[] =                                      \
          #call " failed in child " __FILE__ ":" TOSTRING(__LINE__) "\n";     \
      /* Best effort; there is nothing left to do if this fails too. */       \
      ssize_t written = write(STDERR_FILENO, kMessage, sizeof(kMessage) - 1); \
      (void)written;                                                          \
      _exit(1);                                                               \
    }                                                                         \
  } while (0)

namespace google {
namespace protobuf {
namespace {

using Clock = std::chrono::steady_clock;

// Time left until `deadline`, in whole milliseconds, never negative.
std::chrono::milliseconds RemainingUntil(Clock::time_point deadline) {
  return std::max(std::chrono::duration_cast<std::chrono::milliseconds>(
                      deadline - Clock::now()),
                  std::chrono::milliseconds::zero());
}

// Waits until `fd` is readable (data available, EOF or error) or `timeout`
// elapses.  Returns false on timeout, and also if poll() itself fails with
// anything but EINTR (which can only be a programming error such as a bad fd):
// the caller then takes its timeout path rather than risking a read() that
// blocks indefinitely.
bool WaitReadable(int fd, std::chrono::milliseconds timeout) {
  const Clock::time_point deadline = Clock::now() + timeout;
  while (true) {
    const int timeout_ms = static_cast<int>(
        std::min<int64_t>(RemainingUntil(deadline).count(), INT_MAX));
    pollfd pfd = {};
    pfd.fd = fd;
    pfd.events = POLLIN;
    const int ready = poll(&pfd, 1, timeout_ms);
    if (ready > 0) return true;
    if (ready == 0) return false;
    if (errno != EINTR) {
      ABSL_LOG(ERROR) << "poll() on the testee's pipe failed: "
                      << strerror(errno);
      return false;
    }
  }
}

// Reads what the testee prints on its stdout in response to SIGQUIT (e.g. a
// JVM thread dump), for logging.  Every wait is bounded: reading stops at EOF,
// once the testee has been silent for `kIdleTimeout`, after `kTotalTimeout`
// overall, or when `kMaxBytes` have been read, so a testee that ignores
// SIGQUIT and never writes anything cannot hang the runner.
std::string ReadSigquitOutput(int fd) {
  constexpr size_t kMaxBytes = 5000;
  constexpr std::chrono::milliseconds kIdleTimeout = std::chrono::seconds(1);
  constexpr std::chrono::seconds kTotalTimeout(5);

  std::string out(kMaxBytes, '\0');
  size_t ofs = 0;
  const Clock::time_point deadline = Clock::now() + kTotalTimeout;
  while (ofs < out.size()) {
    const std::chrono::milliseconds remaining = RemainingUntil(deadline);
    if (remaining <= std::chrono::milliseconds::zero()) break;
    if (!WaitReadable(fd, std::min(remaining, kIdleTimeout))) break;
    const ssize_t bytes_read = read(fd, &out[ofs], out.size() - ofs);
    if (bytes_read < 0 && errno == EINTR) continue;
    if (bytes_read <= 0) break;  // EOF or error.
    ofs += static_cast<size_t>(bytes_read);
  }
  out.resize(ofs);
  return out;
}

}  // namespace

struct ForkPipeRunner::State {
  // Ends of the to-testee and from-testee pipes owned by this process; -1 when
  // not open.
  int write_fd = -1;
  int read_fd = -1;
  // -1 when there is no live, unreaped testee.
  pid_t child_pid = -1;
};

ForkPipeRunner::ForkPipeRunner(absl::string_view executable,
                               absl::Span<const std::string> executable_args)
    : executable_(executable),
      executable_args_(executable_args.begin(), executable_args.end()),
      state_(std::make_unique<State>()) {}

ForkPipeRunner::ForkPipeRunner(absl::string_view executable)
    : executable_(executable), state_(std::make_unique<State>()) {}

ForkPipeRunner::~ForkPipeRunner() { Shutdown(shutdown_grace_period_); }

bool ForkPipeRunner::IsTestProgramRunning() const {
  return state_->child_pid >= 0;
}

void ForkPipeRunner::SpawnTestProgram() {
  int toproc_pipe_fd[2];
  int fromproc_pipe_fd[2];
  if (pipe(toproc_pipe_fd) < 0 || pipe(fromproc_pipe_fd) < 0) {
    perror("pipe");
    exit(1);
  }
  // Keep the pipes out of any other program this process execs, e.g. the
  // testee of a second ForkPipeRunner: a stray copy of the write end would
  // hold this testee's stdin open after Shutdown() closes ours, so it would
  // never see EOF and would have to be killed.  The child's dup2() below
  // clears the flag on its own copies.  (pipe2(O_CLOEXEC) would close the
  // window between pipe() and fcntl(), but macOS doesn't have it.)
  for (int fd : {toproc_pipe_fd[0], toproc_pipe_fd[1], fromproc_pipe_fd[0],
                 fromproc_pipe_fd[1]}) {
    CHECK_SYSCALL(fcntl(fd, F_SETFD, FD_CLOEXEC));
  }

  // Build argv and log it *before* fork(): this process is multi-threaded, so
  // the child must only make async-signal-safe calls (no logging) until execv.
  std::vector<const char*> argv;
  argv.reserve(executable_args_.size() + 2);
  argv.push_back(executable_.c_str());
  ABSL_LOG(INFO) << argv[0];
  for (size_t i = 0; i < executable_args_.size(); ++i) {
    argv.push_back(executable_args_[i].c_str());
    ABSL_LOG(INFO) << executable_args_[i];
  }
  argv.push_back(nullptr);

  pid_t pid = fork();
  if (pid < 0) {
    perror("fork");
    exit(1);
  }

  if (pid) {
    // Parent.
    CHECK_SYSCALL(close(toproc_pipe_fd[0]));
    CHECK_SYSCALL(close(fromproc_pipe_fd[1]));
    state_->write_fd = toproc_pipe_fd[1];
    state_->read_fd = fromproc_pipe_fd[0];
    state_->child_pid = pid;
  } else {
    // Child.
    CHECK_SYSCALL_IN_CHILD(close(STDIN_FILENO));
    CHECK_SYSCALL_IN_CHILD(close(STDOUT_FILENO));
    CHECK_SYSCALL_IN_CHILD(dup2(toproc_pipe_fd[0], STDIN_FILENO));
    CHECK_SYSCALL_IN_CHILD(dup2(fromproc_pipe_fd[1], STDOUT_FILENO));

    CHECK_SYSCALL_IN_CHILD(close(toproc_pipe_fd[0]));
    CHECK_SYSCALL_IN_CHILD(close(fromproc_pipe_fd[1]));
    CHECK_SYSCALL_IN_CHILD(close(toproc_pipe_fd[1]));
    CHECK_SYSCALL_IN_CHILD(close(fromproc_pipe_fd[0]));

    // Become the leader of a new process group (setpgid() is
    // async-signal-safe), so that Shutdown() can SIGKILL the group and reach
    // any process the testee forks.  See the class comment for the trade-off.
    CHECK_SYSCALL_IN_CHILD(setpgid(0, 0));

    // Never returns.
    CHECK_SYSCALL_IN_CHILD(
        execv(executable_.c_str(), const_cast<char**>(argv.data())));
  }
}

ForkPipeRunner::ShutdownResult ForkPipeRunner::Shutdown(
    std::chrono::milliseconds grace_period) {
  // Closing our end of the testee's stdin makes it see EOF, which is how a
  // conformance testee learns that the run is over and exits.  Closing our end
  // of its stdout too means a testee blocked writing a response to a full pipe
  // gets SIGPIPE/EPIPE instead of hanging forever.  The fds are reset to -1 so
  // a second call (e.g. the destructor after RunTest()'s crash path) is a
  // no-op.
  if (state_->write_fd >= 0) {
    close(state_->write_fd);
    state_->write_fd = -1;
  }
  if (state_->read_fd >= 0) {
    close(state_->read_fd);
    state_->read_fd = -1;
  }

  ShutdownResult result;
  if (state_->child_pid <= 0) return result;
  const pid_t pid = state_->child_pid;
  state_->child_pid = -1;

  // Give the testee a bounded grace period to exit on its own after EOF.  A
  // blocking waitpid() would let a misbehaving testee (one that ignores EOF)
  // hang the runner, so poll with WNOHANG instead and escalate to SIGKILL,
  // which cannot be ignored, once the deadline passes.  Only after a
  // successful SIGKILL do we wait synchronously, and only for the kill to be
  // delivered.
  //
  // The SIGKILL goes to the testee's process group (it made itself the leader
  // of one in SpawnTestProgram()), so it also reaches anything the testee
  // forked, such as the real testee behind a wrapper script that does not
  // exec it.
  //
  // TODO: b/563658894 - The group is only signalled if the direct child is
  // still alive when the grace period ends.  If it exits on its own (e.g. a
  // wrapper script that exits on EOF after backgrounding the real testee),
  // whatever it left behind in the group survives and only learns the run is
  // over from EOF on stdin or SIGPIPE on stdout.  Sweeping the group after the
  // reap would race with pid reuse; doing it safely needs waitid(WNOWAIT) to
  // look at the status before reaping.
  constexpr std::chrono::milliseconds kPollInterval(10);
  // How long to keep polling for the testee to disappear if kill() failed.
  constexpr std::chrono::seconds kKillFailureWait(1);
  enum class Phase {
    kGrace,       // Waiting for the testee to exit on its own.
    kKilled,      // SIGKILL sent; a blocking waitpid() is now safe.
    kKillFailed,  // kill() failed; polling a little longer, then giving up.
  };
  Phase phase = Phase::kGrace;
  auto deadline = std::chrono::steady_clock::now() + grace_period;
  while (true) {
    int status = 0;
    const pid_t reaped =
        waitpid(pid, &status, phase == Phase::kKilled ? 0 : WNOHANG);
    if (reaped == pid) {
      result.wait_status = status;
      return result;
    }
    if (reaped < 0) {
      if (errno == EINTR) continue;
      // ECHILD means there is no such child to reap (e.g. something else in
      // the process already reaped it).  Anything else is unexpected, but
      // there is nothing useful left to do about it during shutdown.
      if (errno != ECHILD) {
        ABSL_LOG(WARNING) << "waitpid(" << pid
                          << ") failed: " << strerror(errno);
      }
      return result;
    }
    // reaped == 0: the testee is still running.
    const auto now = std::chrono::steady_clock::now();
    if (now < deadline) {
      std::this_thread::sleep_for(kPollInterval);
      continue;
    }
    if (phase == Phase::kKillFailed) {
      ABSL_LOG(ERROR) << "giving up on child pid=" << pid
                      << ", which could not be killed and has not exited";
      return result;
    }
    if (grace_period > std::chrono::milliseconds::zero()) {
      ABSL_LOG(WARNING) << "child pid=" << pid << " did not exit within "
                        << std::chrono::duration<double>(grace_period).count()
                        << "s of its pipes being closed, sending SIGKILL";
    } else {
      // With no grace period the child may be exiting as we speak (its pipes
      // were closed microseconds ago); the SIGKILL is then merely redundant.
      ABSL_LOG(INFO) << "child pid=" << pid
                     << " has not exited yet, sending SIGKILL (no grace "
                        "period)";
    }
    // kill(-pid) fails with ESRCH if the group does not exist, which can only
    // happen if the child has not reached its setpgid() yet; then signal the
    // child alone.
    if (kill(-pid, SIGKILL) == 0 || kill(pid, SIGKILL) == 0) {
      phase = Phase::kKilled;  // The next waitpid() blocks until it lands.
      result.killed = true;
      continue;
    }
    // ESRCH would mean the child is gone, in which case waitpid() reports
    // ECHILD (or reaps it) on the next iteration.  EPERM should be impossible
    // for our own child; rather than risk a blocking waitpid() that never
    // returns, poll a little longer and then give up.
    ABSL_LOG(ERROR) << "kill(" << pid
                    << ", SIGKILL) failed: " << strerror(errno);
    phase = Phase::kKillFailed;
    deadline = now + kKillFailureWait;
  }
}

std::string ForkPipeRunner::GetTestProgramFailure(ReadResult read_result) {
  // The testee produced no response: it exited, crashed, or hung.  After EOF
  // or a read error there is nothing to wait politely for, so it is shut
  // down with no grace period (SIGKILL at once if it is still alive).  After
  // a timeout it has just been sent SIGQUIT; a testee whose default action
  // is to dump core is given a moment to finish dying, so that its wait
  // status (and its core) reflect the SIGQUIT rather than our SIGKILL.
  // Either way the outcome is classified from the wait status.
  constexpr std::chrono::seconds kTimeoutGracePeriod(2);
  ABSL_LOG(INFO) << "Trying to reap child, pid=" << state_->child_pid;
  const ShutdownResult shutdown = Shutdown(
      read_result == ReadResult::kTimeout ? kTimeoutGracePeriod
                                          : std::chrono::milliseconds::zero());
  const absl::optional<int>& status = shutdown.wait_status;
  const bool signaled = status.has_value() && WIFSIGNALED(*status);
  const bool exited = status.has_value() && WIFEXITED(*status);

  std::string error_msg;
  if (read_result == ReadResult::kTimeout) {
    // Signals sent by the runner itself are reported as such, so that they
    // are not mistaken for the testee crashing on its own.
    error_msg = "child timed out";
    if (shutdown.killed && signaled && WTERMSIG(*status) == SIGKILL) {
      error_msg += " (killed by runner)";
    } else if (signaled && WTERMSIG(*status) == SIGQUIT) {
      error_msg += " (terminated by runner's SIGQUIT)";
    } else if (signaled) {
      absl::StrAppendFormat(&error_msg, ", died with signal %d",
                            WTERMSIG(*status));
    } else if (exited) {
      absl::StrAppendFormat(&error_msg, ", exited with status=%d",
                            WEXITSTATUS(*status));
    }
  } else if (signaled) {
    absl::StrAppendFormat(&error_msg, "child killed by signal %d",
                          WTERMSIG(*status));
  } else if (exited) {
    absl::StrAppendFormat(&error_msg, "child exited, status=%d",
                          WEXITSTATUS(*status));
  } else {
    // Nothing to reap (or waitpid() failed); all we know is the read failed.
    error_msg = read_result == ReadResult::kEof
                    ? "child closed its output without responding"
                    : "error reading from child";
  }
  return error_msg;
}

void ForkPipeRunner::CheckedWrite(const void* buf, size_t len) {
  if (static_cast<size_t>(write(state_->write_fd, buf, len)) != len) {
    ABSL_LOG(FATAL) << current_test_name_
                    << ": error writing to test program: " << strerror(errno);
  }
}

ForkPipeRunner::ReadResult ForkPipeRunner::TryRead(void* buf, size_t len) {
  // The timeout is implemented with poll() on the calling thread rather than
  // by racing a blocking read() on a helper thread against a deadline: a
  // helper thread stuck in read() on a testee that never answers would have to
  // be joined (blocking us) or leaked, whereas poll() leaves nothing behind.
  const int fd = state_->read_fd;
  char* out = static_cast<char*>(buf);
  size_t ofs = 0;
  while (ofs < len) {
    if (!WaitReadable(fd, read_timeout_)) {
      ABSL_LOG(ERROR) << current_test_name_ << ": timeout from test program";
      if (state_->child_pid > 0) {
        // Some runtimes (notably the JVM) react to SIGQUIT by dumping all
        // their threads' stacks to stdout, i.e. into our pipe; log whatever
        // arrives, for a bounded time, to help diagnose the hang.  The caller
        // then shuts the testee down.  The signal goes to the testee's process
        // group so that it reaches the real testee behind a wrapper script
        // too (see the class comment for the trade-off); as in Shutdown(),
        // kill(-pid) fails with ESRCH only if the child has not reached its
        // setpgid() yet, in which case the child alone is signaled.
        const pid_t pid = state_->child_pid;
        if (kill(-pid, SIGQUIT) != 0) kill(pid, SIGQUIT);
        // TODO: Only log in flag-guarded mode, since reading output from
        // SIGQUIT is slow and verbose.
        const std::string sigquit_output = ReadSigquitOutput(fd);
        ABSL_LOG(ERROR) << "child pid=" << pid << " SIGQUIT: \n"
                        << (sigquit_output.empty() ? "(no output)"
                                                   : sigquit_output);
      }
      return ReadResult::kTimeout;
    }

    const ssize_t bytes_read = read(fd, out + ofs, len - ofs);
    if (bytes_read == 0) {
      ABSL_LOG(ERROR) << current_test_name_
                      << ": unexpected EOF from test program";
      return ReadResult::kEof;
    } else if (bytes_read < 0) {
      if (errno == EINTR) continue;
      ABSL_LOG(ERROR) << current_test_name_
                      << ": error reading from test program: "
                      << strerror(errno);
      return ReadResult::kError;
    }

    ofs += static_cast<size_t>(bytes_read);
  }

  return ReadResult::kOk;
}

void ForkPipeRunner::CheckedRead(void* buf, size_t len) {
  // TODO: b/563658894 - classify mid-body read timeouts like header-read
  // timeouts instead of crashing the runner.
  if (TryRead(buf, len) != ReadResult::kOk) {
    ABSL_LOG(FATAL) << current_test_name_
                    << ": error reading from test program: " << strerror(errno);
  }
}

}  // namespace protobuf
}  // namespace google

#endif  // !_WIN32
