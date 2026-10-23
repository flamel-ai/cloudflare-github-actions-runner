import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vite-plus/test";

import { AccountRunnerScheduler, type SchedulerJobInput } from "../../src/account-runner-scheduler";
import { runnerCacheTokenLifetimeMs } from "../../src/runner-cache";
import { RUNNER_PROFILES } from "../../src/runner-profiles";

const target = { owner: "biw", repository: "runner-poc" };
const profile = RUNNER_PROFILES["standard-3"];

function job(jobId: string, runnerName: string, cacheScope: string): SchedulerJobInput {
  return {
    jobId,
    headSha: "0123456789abcdef0123456789abcdef01234567",
    runnerName,
    target,
    installationId: 42,
    profile,
    workerOrigin: "https://runner.example.workers.dev",
    cacheScope: { scope: cacheScope, fallbackScope: "refs/heads/main", writeAllowed: true },
  };
}

async function prepareRunner(
  scheduler: DurableObjectStub<import("../../src/account-runner-scheduler").AccountRunnerScheduler>,
  jobId: string,
): Promise<void> {
  await runInDurableObject(scheduler, async (_instance, state) => {
    // Admission capacity is separately covered by scheduler-policy tests. This
    // test prepares two live, compatible JIT runners to reproduce GitHub's
    // out-of-order assignment race without calling the Containers API. Mark
    // the pending capacity operation as already applied so the Durable Object
    // alarm cannot race this test and issue a real capacity request.
    state.storage.sql.exec(
      `UPDATE scheduler_slots
       SET applied_max_instances = desired_max_instances,
           capacity_debounce_until = 0,
           capacity_update_in_progress = 0
       WHERE slot_id = 'preset:standard-3'`,
    );
    state.storage.sql.exec(
      "UPDATE scheduler_jobs SET status = 'provisioning', slot_id = 'preset:standard-3' WHERE job_id = ?",
      jobId,
    );
  });
}

async function provisionRunner(
  scheduler: DurableObjectStub<import("../../src/account-runner-scheduler").AccountRunnerScheduler>,
  jobId: string,
  runnerName: string,
  runnerId: number,
): Promise<void> {
  await prepareRunner(scheduler, jobId);
  await scheduler.runnerProvisioned(jobId, runnerName, runnerId);
  await scheduler.runnerStarted(runnerName);
}

describe("AccountRunnerScheduler JIT cache assignments", () => {
  it.each([false, true])("replays assignment before queued delivery with completed source %s", async (completed) => {
    const scheduler = env.RUNNER_SCHEDULER.getByName(`pending-assignment-${completed}`);
    const source = job("610", "cf-standard-3-job-610", "refs/pull/610/merge");
    const assigned = job("620", "cf-standard-3-job-620", "refs/pull/620/merge");
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, 6101);
    if (completed) await scheduler.workflowJobCompleted(source.jobId);
    const claim = { jobId: assigned.jobId, runnerName: source.runnerName, runnerId: 6101, target, profile };
    await scheduler.workflowJobStarted(claim);
    await expect(scheduler.cacheAssignment(source.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
    await scheduler.submit(assigned);
    await expect(scheduler.cacheAssignment(source.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: assigned.jobId,
      cacheScope: assigned.cacheScope,
    });
    await scheduler.workflowJobStarted(claim);
    await scheduler.submit(assigned);
    await runInDurableObject(scheduler, async (_instance, state) => {
      const row = state.storage.sql
        .exec<{ status: string; github_completed_at: number | null }>(
          "SELECT status, github_completed_at FROM scheduler_jobs WHERE job_id = ?",
          source.jobId,
        )
        .one();
      expect(completed ? ["completed"] : ["queued", "admitted"]).toContain(row.status);
      const actual = state.storage.sql
        .exec<{ status: string }>("SELECT status FROM scheduler_jobs WHERE job_id = ?", assigned.jobId)
        .one();
      expect(actual.status).toBe("running");
    });
  });

  it.each([false, true])(
    "retains pending physical reservation across restart and recovery with completion first %s",
    async (completionFirst) => {
      const scheduler = env.RUNNER_SCHEDULER.getByName(`pending-recovery-${completionFirst}`);
      const source = job("710", "cf-standard-3-job-710", "refs/pull/710/merge");
      const assigned = job("720", "cf-standard-3-job-720", "refs/pull/720/merge");
      await scheduler.submit(source);
      await provisionRunner(scheduler, source.jobId, source.runnerName, 7101);
      if (completionFirst) await scheduler.workflowJobCompleted(source.jobId);
      await scheduler.workflowJobStarted({
        jobId: assigned.jobId,
        runnerName: source.runnerName,
        runnerId: 7101,
        target,
        profile,
      });
      if (!completionFirst) await scheduler.workflowJobCompleted(source.jobId);
      await runInDurableObject(scheduler, async (_instance, state) => {
        state.storage.sql.exec(
          "UPDATE scheduler_jobs SET recovery_due_at = ? WHERE job_id = ?",
          Date.now() - 1,
          source.jobId,
        );
        state.storage.sql.exec(
          "UPDATE scheduler_jobs SET runner_cleanup_state = 'retry', runner_cleanup_due_at = ? WHERE job_id = ?",
          Date.now() - 1,
          source.jobId,
        );
        const restarted = new AccountRunnerScheduler(state, env);
        await state.blockConcurrencyWhile(async () => {});
        await restarted.alarm();
        const row = state.storage.sql
          .exec<{ status: string; slot_id: string | null; runner_id: number | null }>(
            "SELECT status,slot_id,runner_id FROM scheduler_jobs WHERE job_id = ?",
            source.jobId,
          )
          .one();
        expect(row.status).toBe("releasing");
        expect(row.slot_id).toBe("preset:standard-3");
        expect(row.runner_id).toBe(7101);
      });
      await expect(scheduler.cacheAssignment(source.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
      await scheduler.submit(assigned);
      await runInDurableObject(scheduler, async (_instance, state) => {
        const row = state.storage.sql
          .exec<{ status: string; slot_id: string | null }>(
            "SELECT status,slot_id FROM scheduler_jobs WHERE job_id = ?",
            assigned.jobId,
          )
          .one();
        expect(row).toEqual({ status: "running", slot_id: "preset:standard-3" });
      });
      await scheduler.runnerStopped(source.runnerName, { exitCode: 0, reason: "actual complete" });
      await scheduler.workflowJobCompleted(assigned.jobId);
      await runInDurableObject(scheduler, async (_instance, state) => {
        const row = state.storage.sql
          .exec<{ status: string }>("SELECT status FROM scheduler_jobs WHERE job_id = ?", assigned.jobId)
          .one();
        expect(row.status).toBe("completed");
      });
    },
  );

  it.each(["stop", "completion", "expiry"])(
    "ends pending reservation after %s without granting early cache access",
    async (ending) => {
      const scheduler = env.RUNNER_SCHEDULER.getByName(`pending-end-${ending}`);
      const source = job("730", "cf-standard-3-job-730", "refs/pull/730/merge");
      const assigned = job("740", "cf-standard-3-job-740", "refs/pull/740/merge");
      await scheduler.submit(source);
      await provisionRunner(scheduler, source.jobId, source.runnerName, 7301);
      await scheduler.workflowJobCompleted(source.jobId);
      const claim = { jobId: assigned.jobId, runnerName: source.runnerName, runnerId: 7301, target, profile };
      await scheduler.workflowJobStarted(claim);
      if (ending === "stop") await scheduler.runnerStopped(source.runnerName, { exitCode: 0, reason: "physical stop" });
      else {
        if (ending === "completion") await scheduler.workflowJobCompleted(assigned.jobId);
        await runInDurableObject(scheduler, async (_instance, state) => {
          if (ending === "expiry")
            state.storage.sql.exec(
              "UPDATE scheduler_jit_runners SET created_at = ? WHERE runner_name = ?",
              Date.now() - runnerCacheTokenLifetimeMs - 1,
              source.runnerName,
            );
          state.storage.sql.exec(
            "UPDATE scheduler_jobs SET runner_id = NULL, recovery_due_at = ? WHERE job_id = ?",
            Date.now() - 1,
            source.jobId,
          );
        });
        await runInDurableObject(scheduler, async (instance) => {
          await instance.alarm();
        });
      }
      await expect(scheduler.cacheAssignment(source.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
      await runInDurableObject(scheduler, async (_instance, state) => {
        const row = state.storage.sql
          .exec<{ status: string }>("SELECT status FROM scheduler_jobs WHERE job_id = ?", source.jobId)
          .one();
        expect(row.status).toBe("completed");
      });
      await scheduler.submit(assigned);
      await runInDurableObject(scheduler, async (_instance, state) => {
        const row = state.storage.sql
          .exec<{ status: string; slot_id: string | null }>(
            "SELECT status,slot_id FROM scheduler_jobs WHERE job_id = ?",
            assigned.jobId,
          )
          .one();
        expect(row.status).not.toBe("running");
      });
    },
  );

  it.each([false, true])("completion before queued delivery wins with start first %s", async (startFirst) => {
    const scheduler = env.RUNNER_SCHEDULER.getByName(`pending-completion-${startFirst}`);
    const source = job("630", "cf-standard-3-job-630", "refs/pull/630/merge");
    const assigned = job("640", "cf-standard-3-job-640", "refs/pull/640/merge");
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, 6301);
    const claim = { jobId: assigned.jobId, runnerName: source.runnerName, runnerId: 6301, target, profile };
    if (startFirst) await scheduler.workflowJobStarted(claim);
    await scheduler.workflowJobCompleted(assigned.jobId);
    const firstCompletion = await runInDurableObject(scheduler, async (_instance, state) => {
      return state.storage.sql
        .exec<{ created_at: number }>(
          "SELECT created_at FROM scheduler_events WHERE job_id = ? AND kind = 'github-job-completed-before-queue'",
          assigned.jobId,
        )
        .one().created_at;
    });
    await scheduler.workflowJobStarted(claim);
    await scheduler.workflowJobCompleted(assigned.jobId);
    const result = await scheduler.submit(assigned);
    expect(result.admissions).toEqual([]);
    await expect(scheduler.cacheAssignment(source.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
    await runInDurableObject(scheduler, async (_instance, state) => {
      const row = state.storage.sql
        .exec<{ status: string; github_completed_at: number }>(
          "SELECT status, github_completed_at FROM scheduler_jobs WHERE job_id = ?",
          assigned.jobId,
        )
        .one();
      expect(row.status).toBe("cancelled");
      expect(row.github_completed_at).toBe(firstCompletion);
    });
  });

  it("refuses conflicting pending claims and mismatched queued authorization", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("pending-assignment-boundaries");
    const source = job("650", "cf-standard-3-job-650", "refs/pull/650/merge");
    const assigned = job("660", "cf-standard-3-job-660", "refs/pull/660/merge");
    await scheduler.submit(source);
    await provisionRunner(scheduler, source.jobId, source.runnerName, 6501);
    const claim = { jobId: assigned.jobId, runnerName: source.runnerName, runnerId: 6501, target, profile };
    await expect(
      scheduler.workflowJobStarted({ ...claim, target: { ...target, repository: "other" } }),
    ).resolves.toMatchObject({ accepted: false });
    await expect(
      scheduler.workflowJobStarted({ ...claim, profile: RUNNER_PROFILES["standard-2"] }),
    ).resolves.toMatchObject({ accepted: false });
    await scheduler.workflowJobStarted(claim);
    await expect(scheduler.workflowJobStarted({ ...claim, jobId: "670" })).resolves.toMatchObject({ accepted: false });
    await scheduler.submit({ ...assigned, target: { ...target, repository: "other" } });
    await expect(scheduler.cacheAssignment(source.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
  });

  it("records a JIT runner when its source job completes during creation", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("completed-during-jit-creation");
    const sourceJob = job("510", "cf-standard-3-job-510", "refs/pull/510/merge");
    const assignedJob = job("520", "cf-standard-3-job-520", "refs/pull/520/merge");

    await scheduler.submit(sourceJob);
    await scheduler.submit(assignedJob);
    await prepareRunner(scheduler, sourceJob.jobId);
    await scheduler.workflowJobCompleted(sourceJob.jobId);
    await scheduler.runnerProvisioned(sourceJob.jobId, sourceJob.runnerName, 5_101);
    await scheduler.runnerStopped(sourceJob.runnerName, { exitCode: 0, reason: "source reservation released" });

    await expect(scheduler.cacheAssignment(sourceJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
    await expect(
      scheduler.workflowJobStarted({
        jobId: assignedJob.jobId,
        runnerName: sourceJob.runnerName,
        runnerId: 5_101,
        target,
        profile,
      }),
    ).resolves.toEqual({ accepted: true, admissions: [] });

    await expect(scheduler.cacheAssignment(sourceJob.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: assignedJob.jobId,
      cacheScope: assignedJob.cacheScope,
    });
    await scheduler.runnerProvisioned(sourceJob.jobId, sourceJob.runnerName, 5_101);
    await expect(scheduler.cacheAssignment(sourceJob.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: assignedJob.jobId,
      cacheScope: assignedJob.cacheScope,
    });
  });

  it("does not extend completion grace after duplicate webhooks or unrelated updates", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("completed-jit-cache-grace");
    const queuedJob = job("530", "cf-standard-3-job-530", "refs/pull/530/merge");

    await scheduler.submit(queuedJob);
    await provisionRunner(scheduler, queuedJob.jobId, queuedJob.runnerName, 5_301);
    await scheduler.workflowJobStarted({
      jobId: queuedJob.jobId,
      runnerName: queuedJob.runnerName,
      runnerId: 5_301,
      target,
      profile,
    });
    await scheduler.workflowJobCompleted(queuedJob.jobId);
    await scheduler.runnerStopped(queuedJob.runnerName, { exitCode: 0, reason: "job completed" });

    await expect(scheduler.cacheAssignment(queuedJob.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: queuedJob.jobId,
      cacheScope: queuedJob.cacheScope,
    });
    const completedAt = Date.now() - 5 * 60_000 - 1_000;
    await runInDurableObject(scheduler, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET github_completed_at = ?, updated_at = ? WHERE job_id = ?",
        completedAt,
        Date.now(),
        queuedJob.jobId,
      );
    });
    await scheduler.workflowJobCompleted(queuedJob.jobId);
    await expect(scheduler.cacheAssignment(queuedJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
    await scheduler.workflowJobStarted({
      jobId: queuedJob.jobId,
      runnerName: queuedJob.runnerName,
      runnerId: 5_301,
      target,
      profile,
    });
    await runInDurableObject(scheduler, async (_instance, state) => {
      const row = state.storage.sql
        .exec<{ github_completed_at: number }>(
          "SELECT github_completed_at FROM scheduler_jobs WHERE job_id = ?",
          queuedJob.jobId,
        )
        .one();
      expect(row.github_completed_at).toBe(completedAt);
    });
    await expect(scheduler.cacheAssignment(queuedJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
    await expect(
      scheduler.cacheScope(queuedJob.runnerName, "biw/runner-poc", queuedJob.jobId),
    ).resolves.toBeUndefined();
    await runInDurableObject(scheduler, async (_instance, state) => {
      state.storage.sql.exec("DELETE FROM scheduler_jit_runners WHERE runner_name = ?", queuedJob.runnerName);
    });
    await expect(scheduler.cacheAssignment(queuedJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
  });

  it("keeps an assigned runner authorized when replacement provisioning fails until GitHub completes", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("replacement-failure-with-live-assignment");
    const sourceJob = job("540", "cf-standard-3-job-540", "refs/pull/540/merge");
    const assignedJob = job("550", "cf-standard-3-job-550", "refs/pull/550/merge");

    await scheduler.submit(sourceJob);
    await scheduler.submit(assignedJob);
    await provisionRunner(scheduler, sourceJob.jobId, sourceJob.runnerName, 5_401);
    await provisionRunner(scheduler, assignedJob.jobId, assignedJob.runnerName, 5_501);
    await scheduler.workflowJobStarted({
      jobId: assignedJob.jobId,
      runnerName: sourceJob.runnerName,
      runnerId: 5_401,
      target,
      profile,
    });
    await scheduler.provisioningFailed(assignedJob.jobId, "replacement JIT creation failed");

    await expect(scheduler.cacheAssignment(sourceJob.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: assignedJob.jobId,
      cacheScope: assignedJob.cacheScope,
    });
    await expect(scheduler.cacheScope(sourceJob.runnerName, "biw/runner-poc", assignedJob.jobId)).resolves.toEqual(
      assignedJob.cacheScope,
    );
    await scheduler.workflowJobCompleted(assignedJob.jobId);
    await runInDurableObject(scheduler, async (_instance, state) => {
      const row = state.storage.sql
        .exec<{ github_completed_at: number | null; status: string }>(
          "SELECT github_completed_at, status FROM scheduler_jobs WHERE job_id = ?",
          assignedJob.jobId,
        )
        .one();
      expect(row.status).toBe("failed");
      expect(row.github_completed_at).toBeTypeOf("number");
      state.storage.sql.exec(
        "UPDATE scheduler_jobs SET github_completed_at = ? WHERE job_id = ?",
        Date.now() - 5 * 60_000 - 1_000,
        assignedJob.jobId,
      );
    });

    await expect(scheduler.cacheAssignment(sourceJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
  });

  it("carries the queued head SHA into the provisioning plan", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("head-sha-provisioning");
    const queuedJob = job("50", "cf-standard-3-job-50", "refs/heads/main");

    await scheduler.submit(queuedJob);
    await runInDurableObject(scheduler, async (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE scheduler_jobs SET status = 'provisioning', slot_id = 'preset:standard-3' WHERE job_id = ?`,
        queuedJob.jobId,
      );
      state.storage.sql.exec(
        `UPDATE scheduler_slots
         SET applied_max_instances = desired_max_instances,
             capacity_update_in_progress = 0,
             capacity_reclaim_pending = 0
         WHERE slot_id = 'preset:standard-3'`,
      );
    });

    await expect(scheduler.claimProvisioning(queuedJob.jobId)).resolves.toMatchObject({
      kind: "provision",
      jobId: queuedJob.jobId,
      headSha: queuedJob.headSha,
    });
  });

  it("keeps cache access with each runner when two compatible JIT runners cross-assign jobs", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("cross-assignment");
    const firstJob = job("100", "cf-standard-3-job-100", "refs/pull/100/merge");
    const secondJob = job("200", "cf-standard-3-job-200", "refs/pull/200/merge");

    await scheduler.submit(firstJob);
    await scheduler.submit(secondJob);
    await provisionRunner(scheduler, firstJob.jobId, firstJob.runnerName, 1_001);
    await provisionRunner(scheduler, secondJob.jobId, secondJob.runnerName, 2_001);

    // GitHub gives runner 100 job 200 first. The scheduler requeues job 100
    // under a new JIT name, so runner 200 no longer has a job row named after
    // it when GitHub later assigns runner 200 to job 100.
    await scheduler.workflowJobStarted({
      jobId: secondJob.jobId,
      runnerName: firstJob.runnerName,
      runnerId: 1_001,
      target,
      profile,
    });
    await scheduler.workflowJobStarted({
      jobId: firstJob.jobId,
      runnerName: secondJob.runnerName,
      runnerId: 2_001,
      target,
      profile,
    });

    await expect(scheduler.cacheAssignment(firstJob.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: secondJob.jobId,
      cacheScope: { scope: secondJob.cacheScope?.scope, fallbackScope: "refs/heads/main", writeAllowed: true },
    });
    await expect(scheduler.cacheAssignment(secondJob.runnerName, "biw/runner-poc")).resolves.toEqual({
      jobId: firstJob.jobId,
      cacheScope: { scope: firstJob.cacheScope?.scope, fallbackScope: "refs/heads/main", writeAllowed: true },
    });
  });

  it("does not grant cache access before GitHub confirms the runner assignment", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("unassigned-runner");
    const queuedJob = job("300", "cf-standard-3-job-300", "refs/pull/300/merge");

    await scheduler.submit(queuedJob);
    await provisionRunner(scheduler, queuedJob.jobId, queuedJob.runnerName, 3_001);

    await expect(scheduler.cacheAssignment(queuedJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
  });

  it("does not record an assignment across repository or machine-profile boundaries", async () => {
    const scheduler = env.RUNNER_SCHEDULER.getByName("isolated-assignment");
    const queuedJob = job("400", "cf-standard-3-job-400", "refs/pull/400/merge");

    await scheduler.submit(queuedJob);
    await provisionRunner(scheduler, queuedJob.jobId, queuedJob.runnerName, 4_001);
    await scheduler.workflowJobStarted({
      jobId: queuedJob.jobId,
      runnerName: queuedJob.runnerName,
      runnerId: 4_001,
      target: { owner: "biw", repository: "different-repository" },
      profile,
    });

    await expect(scheduler.cacheAssignment(queuedJob.runnerName, "biw/runner-poc")).resolves.toBeUndefined();
  });
});
