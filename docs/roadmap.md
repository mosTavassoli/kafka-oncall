# Kafka On-Call roadmap

## Current checkpoint

The deterministic runbook, HolmesGPT investigation skill, and browser
replay are working. In [one measured run](experiment.md#measured-result),
the agent and runbook both passed five synthetic cases. The next milestone
is to validate the same approach with observations from a running system.

## Next: capture one reproducible scheduling incident

1. Use a disposable local Kubernetes cluster with Kafka, KEDA, and one
   consumer Deployment. This can be a minimal standalone lab or a Failure
   Lab deployment; Kafka On-Call must not require the Failure Lab app.
2. Confirm a healthy run, then create a controlled capacity shortfall: the
   HPA requests more replicas than are ready and the additional pods
   report `PodScheduled=False` with `Insufficient cpu`. Record the known
   cause independently of the agent.
3. Capture and normalize the ScaledObject, HPA, Deployment, and pod
   observations into the existing [`snapshot-v1` contract](design.md#snapshot-contract).
   Capture lag and the KEDA metric when available. Check workload identity
   and timestamps. Verify a manually collected capture before building
   a reusable collector. Store only sanitized lab data.
4. Verify that the runbook replays the capture and cites the scheduling
   evidence. Run HolmesGPT on the same capture when a model is available.
   Save the reproduction steps, the labeled capture, the results, and the
   known limitations. Keep cluster and model access out of offline CI.

**Done when:** someone can reproduce the incident locally, replay the saved
capture offline, and independently check the diagnosis. A live agent result
is a follow-up measurement, not a condition for the offline replay to work.

## Relationship to Failure Lab

Kafka On-Call remains the focused, read-only Kafka/KEDA investigator. Failure
Lab can produce compatible snapshots and display Kafka On-Call findings as
one part of its wider incident workflow. Keep the repositories, contracts,
and tests independent. Neither tool needs production credentials or
automatic remediation to meet this milestone.
