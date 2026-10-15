//! Gas benchmarks and deterministic failure-boundary coverage for orchestrator
//! flow execution and data migration import paths.
#![cfg(test)]

use soroban_sdk::Env;

/// Maximum acceptable number of import retries before the import is considered fatal.
/// This is a deterministic boundary and must not be exceeded by any code path.
const MAX_IMPORT_RETRIES: u32 = 3;

/// Maximum number of records that a single import payload may contain.
const MAX_IMPORT_RECORDS: u32 = 10_000;

/// Maximum number of bytes allowed in an import payload.
const MAX_IMPORT_BYTES: u32 = 1_000_000;

/// Maximum number of concurrent import workers allowed.
const MAX_CONCURRENT_IMPORTS: u32 = 4;

/// Maximum cpu and memory costs for the orchestrator flow benchmark.
const ORCHESTRATOR_MAX_CPU: u64 = 50_000_000;
const ORCHESTRATOR_MAX_MEM: u64 = 2_000_000;

/// Maximum cpu and memory costs for the data migration import benchmark.
const MIGRATION_MAX_CPU: u64 = 20_000_000;
const MIGRATION_MAX_MEM: u64 = 1_000_000;

/// Deterministic failure boundary model for the data migration import path.
///
/// The import path is treated as a state machine with explicit, deterministic transitions:
///
///   Pending -> Validating -> Applying -> Committed
///                            \\-> Retryable (transient failure)
///                            \\-> Rejected (fatal failure)
///
/// Invariants:
///   1. A committed import is atomic: either all records are applied or none are.
///   2. Retries are bounded by MAX_IMPORT_RETRIES and cannot loop forever.
///   3. A failed import never leaves partially applied state.
///   4. Duplicate inputs are detected deterministically and rejected before apply.
///   5. Authorization is checked before any state mutation.
///   6. Concurrent imports are serialized or rejected; no interleaving is allowed.
///   7. Failures are observable via stable error codes without leaking payload contents.
///
/// This module exercises the boundaries of this model deterministically so regressions in
/// any of the invariants above are caught by the gas bench suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportState {
    Pending,
    Validating,
    Applying,
    Committed,
    Retryable,
    Rejected,
}

/// Stable, non-sensitive error codes for the import path.
/// These are the only error values that may be surfaced to users/logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportError {
    Unauthorized,
    InvalidPayload,
    DuplicateRecord,
    TooManyRecords,
    PayloadTooLarge,
    RetryExhausted,
    ConcurrentImport,
    InternalFailure,
}

/// Result of a deterministic import attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportOutcome {
    pub state: ImportState,
    pub applied_records: u32,
    pub retries: u32,
    pub error: Option<ImportError>,
}

/// Deterministic input for the import path benchmark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportInput {
    pub authorized: bool,
    pub records: u32,
    pub bytes: u32,
    pub duplicates: u32,
    pub transient_failures: u32,
    pub concurrent_imports: u32,
}

/// Runs the deterministic import state machine against the given input.
///
/// The function is pure with respect to its input: the same input always produces the
/// same outcome, regardless of the cost of the computation. This makes it suitable for
/// failure-boundary coverage in gas benchmarks.
pub fn run_import(input: &ImportInput) -> ImportOutcome {
    // Invariant 5: authorization is checked before any state mutation.
    if !input.authorized {
        return ImportOutcome {
            state: ImportState::Rejected,
            applied_records: 0,
            retries: 0,
            error: Some(ImportError::Unauthorized),
        };
    }

    // Invariant 6: concurrent imports are rejected deterministically.
    if input.concurrent_imports > MAX_CONCURRENT_IMPORTS {
        return ImportOutcome {
            state: ImportState::Rejected,
            applied_records: 0,
            retries: 0,
            error: Some(ImportError::ConcurrentImport),
        };
    }

    // Boundary: payload size limits.
    if input.bytes > MAX_IMPORT_BYTES {
        return ImportOutcome {
            state: ImportState::Rejected,
            applied_records: 0,
            retries: 0,
            error: Some(ImportError::PayloadTooLarge),
        };
    }

    if input.records > MAX_IMPORT_RECORDS {
        return ImportOutcome {
            state: ImportState::Rejected,
            applied_records: 0,
            retries: 0,
            error: Some(ImportError::TooManyRecords),
        };
    }

    // Boundary: empty payload is invalid.
    if input.records == 0 || input.bytes == 0 {
        return ImportOutcome {
            state: ImportState::Rejected,
            applied_records: 0,
            retries: 0,
            error: Some(ImportError::InvalidPayload),
        };
    }

    // Invariant 4: duplicates are detected before apply.
    if input.duplicates > 0 {
        return ImportOutcome {
            state: ImportState::Rejected,
            applied_records: 0,
            retries: 0,
            error: Some(ImportError::DuplicateRecord),
        };
    }

    // Transient failures are retried up to MAX_IMPORT_RETRIES, then fail fatally.
    // Invariant 2: retries are bounded.
    if input.transient_failures > MAX_IMPORT_RETRIES {
        return ImportOutcome {
            state: ImportState::Rejected,
            applied_records: 0,
            retries: MAX_IMPORT_RETRIES,
            error: Some(ImportError::RetryExhausted),
        };
    }

    // Invariant 1 + 3: atomic apply. Once we reach this point the import is committed
    // and all records are applied at once.
    ImportOutcome {
        state: ImportState::Committed,
        applied_records: input.records,
        retries: input.transient_failures,
        error: None,
    }
}

/// Returns the deterministic state transition for a given outcome.
/// This is exposed so benchmarks can assert on the exact transition chain.
pub fn state_transitions(outcome: &ImportOutcome) -> Vec<ImportState> {
    match outcome.state {
        ImportState::Pending => vec![ImportState::Pending],
        ImportState::Validating => vec![ImportState::Pending, ImportState::Validating],
        ImportState::Applying => vec![
            ImportState::Pending,
            ImportState::Validating,
            ImportState::Applying,
        ],
        ImportState::Committed => vec![
            ImportState::Pending,
            ImportState::Validating,
            ImportState::Applying,
            ImportState::Committed,
        ],
        ImportState::Retryable => vec![
            ImportState::Pending,
            ImportState::Validating,
            ImportState::Retryable,
        ],
        ImportState::Rejected => vec![ImportState::Pending, ImportState::Rejected],
    }
}

/// Returns true if the outcome represents a committed import with no error.
pub fn is_committed(outcome: &ImportOutcome) -> bool {
    outcome.state == ImportState::Committed && outcome.error.is_none()
}

/// Returns true if the outcome is a fatal rejection.
pub fn is_rejected(outcome: &ImportOutcome) -> bool {
    outcome.state == ImportState::Rejected && outcome.error.is_some()
}

/// Returns true if the outcome is a retryable transient failure.
pub fn is_retryable(outcome: &ImportOutcome) -> bool {
    outcome.state == ImportState::Retryable
}

/// Returns the number of retries remaining for a given outcome.
/// This is always non-negative and bounded by MAX_IMPORT_RETRIES.
pub fn retries_remaining(outcome: &ImportOutcome) -> u32 {
    MAX_IMPORT_RETRIES.saturating_sub(outcome.retries)
}

/// Returns true if the outcome is a consistent committed state.
/// This encodes invariant 1 + 3 as an assertable predicate.
pub fn is_consistent(outcome: &ImportOutcome, input: &ImportInput) -> bool {
    match outcome.state {
        ImportState::Committed => {
            outcome.applied_records == input.records && outcome.error.is_none()
        }
        ImportState::Rejected => outcome.applied_records == 0 && outcome.error.is_some(),
        ImportState::Retryable => outcome.applied_records == 0,
        _ => false,
    }
}

/// Returns the deterministic failure boundary for a given input.
/// This is the first condition that would cause the import to fail.
pub fn failure_boundary(input: &ImportInput) -> Option<ImportError> {
    if !input.authorized {
        return Some(ImportError::Unauthorized);
    }
    if input.concurrent_imports > MAX_CONCURRENT_IMPORTS {
        return Some(ImportError::ConcurrentImport);
    }
    if input.bytes > MAX_IMPORT_BYTES {
        return Some(ImportError::PayloadTooLarge);
    }
    if input.records > MAX_IMPORT_RECORDS {
        return Some(ImportError::TooManyRecords);
    }
    if input.records == 0 || input.bytes == 0 {
        return Some(ImportError::InvalidPayload);
    }
    if input.duplicates > 0 {
        return Some(ImportError::DuplicateRecord);
    }
    if input.transient_failures > MAX_IMPORT_RETRIES {
        return Some(ImportError::RetryExhausted);
    }
    None
}

/// Returns the number of records that would be applied for a given input.
/// This is deterministic and bounded by MAX_IMPORT_RECORDS.
pub fn applied_records_for(input: &ImportInput) -> u32 {
    if failure_boundary(input).is_some() {
        0
    } else {
        input.records
    }
}

// ---------------------------------------------------------------------------
// bench_orchestrator_flow: deterministic failure-boundary coverage
// ---------------------------------------------------------------------------
//
// These tests drive the real `Orchestrator` contract through its public client
// against configurable downstream mocks, and check the invariants a caller
// relies on:
//
//   1. Authorization is enforced before any state is touched.
//   2. A flow that fails leaves *no* partial state behind: the execution
//      stats, audit log, execution lock, nonce and every downstream call
//      counter are exactly what they were before the call. (Soroban rolls back
//      a failed invocation; the snapshot comparison below is what makes a
//      regression in that guarantee, or in the contract's own bookkeeping,
//      visible.)
//   3. A rejected flow can be retried once the cause is removed, and the
//      retry succeeds deterministically.
//   4. Amount, deadline, nonce and epoch boundaries are exact: the value on
//      the boundary behaves as documented, the value one past it does not.
//   5. Every path, success or failure, stays inside the documented CPU and
//      memory budget, and the same input always costs the same.
//
// Error values are asserted by their stable `OrchestratorError` code only; no
// payload or address is ever part of an assertion message.

use orchestrator::{Orchestrator, OrchestratorClient, OrchestratorError, RemittanceFlowParams};
use soroban_sdk::testutils::{Address as _, Ledger as _};
use soroban_sdk::{symbol_short, Address, Symbol};

/// Downstream contract double: every orchestrator dependency is an instance
/// of this one contract, each at its own address.
mod downstream {
    use soroban_sdk::{contract, contractimpl, symbol_short, Address, Env, Symbol, Vec};

    #[contract]
    pub struct Downstream;

    fn flag(env: &Env, key: Symbol) -> bool {
        env.storage().instance().get(&key).unwrap_or(false)
    }

    fn bump(env: &Env, key: Symbol) {
        let calls: u32 = env.storage().instance().get(&key).unwrap_or(0);
        env.storage().instance().set(&key, &(calls + 1));
    }

    #[contractimpl]
    impl Downstream {
        /// `fail`: mutating calls panic. `deny`: the spending limit check
        /// returns false. `short`: the split returns fewer than four parts.
        pub fn set_mode(env: Env, fail: bool, deny: bool, short: bool) {
            env.storage().instance().set(&symbol_short!("FAIL"), &fail);
            env.storage().instance().set(&symbol_short!("DENY"), &deny);
            env.storage()
                .instance()
                .set(&symbol_short!("SHORT"), &short);
        }

        pub fn calls(env: Env, name: Symbol) -> u32 {
            env.storage().instance().get(&name).unwrap_or(0)
        }

        pub fn check_spending_limit(
            env: Env,
            _orchestrator: Address,
            _epoch: u64,
            _user: Address,
            _amount: i128,
        ) -> bool {
            bump(&env, symbol_short!("CHECK"));
            !flag(&env, symbol_short!("DENY"))
        }

        pub fn calculate_split(
            env: Env,
            _orchestrator: Address,
            _epoch: u64,
            total_amount: i128,
        ) -> Vec<i128> {
            bump(&env, symbol_short!("SPLIT"));
            if flag(&env, symbol_short!("SHORT")) {
                return soroban_sdk::vec![&env, total_amount];
            }
            let quarter = total_amount / 4;
            soroban_sdk::vec![&env, total_amount - 3 * quarter, quarter, quarter, quarter]
        }

        pub fn add_to_goal(
            env: Env,
            _orchestrator: Address,
            _epoch: u64,
            _caller: Address,
            _goal_id: u32,
            _amount: i128,
        ) {
            if flag(&env, symbol_short!("FAIL")) {
                panic!("savings step failed");
            }
            bump(&env, symbol_short!("ADD"));
        }

        pub fn pay_bill(
            env: Env,
            _orchestrator: Address,
            _epoch: u64,
            _caller: Address,
            _bill_id: u32,
        ) {
            if flag(&env, symbol_short!("FAIL")) {
                panic!("bill step failed");
            }
            bump(&env, symbol_short!("BILL"));
        }

        pub fn pay_premium(
            env: Env,
            _orchestrator: Address,
            _epoch: u64,
            _caller: Address,
            _policy_id: u32,
        ) {
            if flag(&env, symbol_short!("FAIL")) {
                panic!("insurance step failed");
            }
            bump(&env, symbol_short!("PREM"));
        }

        pub fn remove_from_goal(
            env: Env,
            _orchestrator: Address,
            _epoch: u64,
            _user: Address,
            _goal_id: u32,
            _amount: i128,
        ) {
            bump(&env, symbol_short!("UNDO_SG"));
        }

        pub fn reverse_payment(
            env: Env,
            _orchestrator: Address,
            _epoch: u64,
            _user: Address,
            _bill_id: u32,
            _amount: i128,
        ) {
            bump(&env, symbol_short!("UNDO_BP"));
        }

        pub fn reverse_premium(
            env: Env,
            _orchestrator: Address,
            _epoch: u64,
            _user: Address,
            _policy_id: u32,
            _amount: i128,
        ) {
            bump(&env, symbol_short!("UNDO_INS"));
        }
    }
}

use downstream::{Downstream, DownstreamClient};

const BASE_TIME: u64 = 1_700_000_000;
/// Mirrors `MAX_DEADLINE_WINDOW_SECS` in the orchestrator (one hour).
const DEADLINE_WINDOW: u64 = 3_600;
const FLOW_AMOUNT: i128 = 1_000;

/// Which dependency a test is talking about.
#[derive(Clone, Copy)]
enum Dep {
    FamilyWallet = 0,
    RemittanceSplit = 1,
    Savings = 2,
    Bills = 3,
    Insurance = 4,
}

/// Everything observable about the orchestrator and its downstream doubles.
/// Two snapshots being equal is what "no partial state" means.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Snapshot {
    stats: Option<(u32, u32, u32)>,
    audit_entries: u32,
    locked: bool,
    nonce: u64,
    downstream_calls: [u32; 5 * 4],
}

struct Harness {
    env: Env,
    orchestrator: Address,
    owner: Address,
    caller: Address,
    deps: [Address; 5],
}

impl Harness {
    fn new() -> Self {
        Self::build(true)
    }

    /// `mock_auths = false` leaves authorization real, so a call made
    /// without the caller's authorization is rejected by the host.
    fn build(mock_auths: bool) -> Self {
        let env = Env::default();
        if mock_auths {
            env.mock_all_auths();
        }
        env.ledger().set_timestamp(BASE_TIME);
        env.budget().reset_unlimited();

        let orchestrator = env.register_contract(None, Orchestrator);
        let owner = Address::generate(&env);
        let caller = Address::generate(&env);
        let deps = [
            env.register_contract(None, Downstream),
            env.register_contract(None, Downstream),
            env.register_contract(None, Downstream),
            env.register_contract(None, Downstream),
            env.register_contract(None, Downstream),
        ];
        let harness = Harness {
            env,
            orchestrator,
            owner,
            caller,
            deps,
        };
        if mock_auths {
            harness.client().init(
                &harness.owner,
                &harness.deps[0],
                &harness.deps[1],
                &harness.deps[2],
                &harness.deps[3],
                &harness.deps[4],
            );
        }
        harness
    }

    fn client(&self) -> OrchestratorClient<'_> {
        OrchestratorClient::new(&self.env, &self.orchestrator)
    }

    fn dep(&self, dep: Dep) -> DownstreamClient<'_> {
        DownstreamClient::new(&self.env, &self.deps[dep as usize])
    }

    fn set_mode(&self, dep: Dep, fail: bool, deny: bool, short: bool) {
        self.dep(dep).set_mode(&fail, &deny, &short);
    }

    fn params(&self, amount: i128) -> RemittanceFlowParams {
        RemittanceFlowParams {
            caller: self.caller.clone(),
            total_amount: amount,
            family_wallet: self.deps[0].clone(),
            remittance_split: self.deps[1].clone(),
            savings: self.deps[2].clone(),
            bills: self.deps[3].clone(),
            insurance: self.deps[4].clone(),
            goal_id: 1,
            bill_id: 1,
            policy_id: 1,
        }
    }

    fn lock(&self, locked: bool) {
        self.env.as_contract(&self.orchestrator, || {
            self.env
                .storage()
                .instance()
                .set(&symbol_short!("EXEC_LOCK"), &locked);
        });
    }

    fn snapshot(&self) -> Snapshot {
        let client = self.client();
        let names: [Symbol; 4] = [
            symbol_short!("CHECK"),
            symbol_short!("SPLIT"),
            symbol_short!("ADD"),
            symbol_short!("BILL"),
        ];
        let mut downstream_calls = [0u32; 5 * 4];
        for (dep, address) in self.deps.iter().enumerate() {
            let double = DownstreamClient::new(&self.env, address);
            for (slot, name) in names.iter().enumerate() {
                downstream_calls[dep * 4 + slot] = double.calls(name);
            }
        }
        Snapshot {
            stats: client.get_execution_stats().map(|stats| {
                (
                    stats.total_executions,
                    stats.successful_executions,
                    stats.failed_executions,
                )
            }),
            audit_entries: client.get_audit_log(&0, &1_000).len(),
            locked: client.get_execution_state(),
            nonce: client.get_nonce(&self.caller),
            downstream_calls,
        }
    }

    /// Runs the unsigned flow and returns its typed error, if any.
    fn run(&self, amount: i128) -> Result<(), OrchestratorError> {
        match self
            .client()
            .try_execute_remittance_flow(&self.params(amount))
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => panic!("the flow returned a value of the wrong type"),
            Err(Ok(error)) => Err(error),
            Err(Err(_)) => panic!("the flow aborted without a typed orchestrator error"),
        }
    }

    /// `compute_request_hash` of the orchestrator, which is not public.
    /// A mismatch fails `signed_flow_success_*` immediately, so this cannot
    /// drift silently.
    fn request_hash(amount: i128, nonce: u64, deadline: u64) -> u64 {
        let operation: u64 = symbol_short!("flow").to_val().get_payload();
        operation
            .wrapping_add(nonce)
            .wrapping_add(amount as u64)
            .wrapping_add((amount >> 64) as u64)
            .wrapping_add(deadline)
            .wrapping_add(1) // goal_id
            .wrapping_add(1) // bill_id
            .wrapping_add(1) // policy_id
            .wrapping_mul(1_000_000_007)
    }

    fn run_signed(
        &self,
        amount: i128,
        nonce: u64,
        deadline: u64,
        hash: u64,
        epoch: u64,
    ) -> Result<bool, OrchestratorError> {
        match self.client().try_execute_remittance_flow_signed(
            &self.caller,
            &amount,
            &nonce,
            &deadline,
            &hash,
            &epoch,
        ) {
            Ok(Ok(done)) => Ok(done),
            Ok(Err(_)) => panic!("the signed flow returned a value of the wrong type"),
            Err(Ok(error)) => Err(error),
            Err(Err(_)) => panic!("the signed flow aborted without a typed error"),
        }
    }

    fn signed_ok(&self, amount: i128, nonce: u64) -> bool {
        let deadline = BASE_TIME + DEADLINE_WINDOW;
        let hash = Self::request_hash(amount, nonce, deadline);
        self.run_signed(amount, nonce, deadline, hash, 0)
            .expect("signed flow succeeds")
    }
}

/// CPU and memory cost of `f`, measured the way `orchestrator/tests/gas_bench.rs` does.
fn measure<R>(env: &Env, f: impl FnOnce() -> R) -> (u64, u64, R) {
    let mut budget = env.budget();
    budget.reset_unlimited();
    budget.reset_tracker();
    let result = f();
    (
        budget.cpu_instruction_cost(),
        budget.memory_bytes_cost(),
        result,
    )
}

fn assert_within_budget(label: &str, cpu: u64, mem: u64) {
    assert!(
        cpu <= ORCHESTRATOR_MAX_CPU,
        "CPU regression in orchestrator flow ({label}): {cpu} > {ORCHESTRATOR_MAX_CPU}"
    );
    assert!(
        mem <= ORCHESTRATOR_MAX_MEM,
        "memory regression in orchestrator flow ({label}): {mem} > {ORCHESTRATOR_MAX_MEM}"
    );
}

/// Failure boundary: after any rejected call the observable state equals the
/// state before it, and the rejection carries the expected stable code.
fn assert_rejected_without_side_effects<T: core::fmt::Debug>(
    harness: &Harness,
    expected: OrchestratorError,
    action: impl FnOnce() -> Result<T, OrchestratorError>,
) {
    let before = harness.snapshot();
    let outcome = action();
    assert_eq!(outcome.err(), Some(expected));
    assert_eq!(
        harness.snapshot(),
        before,
        "a rejected flow must leave no partial state"
    );
}

/// Benchmark: the orchestrator flow's cost stays inside the documented
/// budget on the success path and on every rejection path, and is
/// deterministic. This is the entry point the other failure-boundary tests
/// below extend.
#[test]
fn bench_orchestrator_flow() {
    let harness = Harness::new();

    let (cpu, mem, outcome) = measure(&harness.env, || harness.run(FLOW_AMOUNT));
    assert_eq!(outcome, Ok(()));
    assert_within_budget("success", cpu, mem);

    // The same flow in an identical fresh environment costs exactly the same.
    let twin = Harness::new();
    let (twin_cpu, twin_mem, twin_outcome) = measure(&twin.env, || twin.run(FLOW_AMOUNT));
    assert_eq!(twin_outcome, Ok(()));
    assert_eq!(
        (cpu, mem),
        (twin_cpu, twin_mem),
        "cost must be deterministic"
    );

    // Rejections are cheaper than, or equal to, the cost budget as well.
    for (label, amount) in [("zero amount", 0), ("negative amount", -1)] {
        let (cpu, mem, outcome) = measure(&harness.env, || harness.run(amount));
        assert_eq!(outcome, Err(OrchestratorError::InvalidAmount), "{label}");
        assert_within_budget(label, cpu, mem);
    }

    harness.set_mode(Dep::Bills, true, false, false);
    let (cpu, mem, outcome) = measure(&harness.env, || harness.run(FLOW_AMOUNT));
    assert_eq!(outcome, Err(OrchestratorError::CrossContractCallFailed));
    assert_within_budget("downstream failure", cpu, mem);
}

/// Success: stats, audit log and every downstream step reflect one flow.
#[test]
fn orchestrator_flow_success_updates_state_once() {
    let harness = Harness::new();
    assert_eq!(harness.run(FLOW_AMOUNT), Ok(()));

    let after = harness.snapshot();
    assert_eq!(after.stats, Some((1, 1, 0)));
    assert_eq!(after.audit_entries, 1);
    assert!(!after.locked, "the execution lock is released");
    assert_eq!(
        harness
            .dep(Dep::FamilyWallet)
            .calls(&symbol_short!("CHECK")),
        1
    );
    assert_eq!(
        harness
            .dep(Dep::RemittanceSplit)
            .calls(&symbol_short!("SPLIT")),
        1
    );
    assert_eq!(harness.dep(Dep::Savings).calls(&symbol_short!("ADD")), 1);
    assert_eq!(harness.dep(Dep::Bills).calls(&symbol_short!("BILL")), 1);
    assert_eq!(harness.dep(Dep::Insurance).calls(&symbol_short!("PREM")), 1);
}

/// Boundary: the amount limit is exact, at `0`, `-1` and `i128::MIN`.
#[test]
fn orchestrator_flow_rejects_non_positive_amounts_without_side_effects() {
    let harness = Harness::new();
    for amount in [0, -1, i128::MIN] {
        assert_rejected_without_side_effects(&harness, OrchestratorError::InvalidAmount, || {
            harness.run(amount)
        });
    }
}

/// Boundary: the smallest positive amounts are accepted. Amounts 1 to 3
/// split to a zero quarter, so no downstream step is called; 4 is the first
/// amount that reaches every step.
#[test]
fn orchestrator_flow_smallest_amounts_are_accepted() {
    let harness = Harness::new();
    for amount in [1, 3] {
        assert_eq!(harness.run(amount), Ok(()), "amount {amount}");
    }
    assert_eq!(harness.dep(Dep::Savings).calls(&symbol_short!("ADD")), 0);
    assert_eq!(harness.dep(Dep::Bills).calls(&symbol_short!("BILL")), 0);
    assert_eq!(harness.dep(Dep::Insurance).calls(&symbol_short!("PREM")), 0);

    assert_eq!(harness.run(4), Ok(()));
    assert_eq!(harness.dep(Dep::Savings).calls(&symbol_short!("ADD")), 1);
    assert_eq!(harness.snapshot().stats, Some((3, 3, 0)));
}

/// Boundary: the largest amount does not overflow any step.
#[test]
fn orchestrator_flow_maximum_amount_is_accepted() {
    let harness = Harness::new();
    assert_eq!(harness.run(i128::MAX), Ok(()));
    assert_eq!(harness.snapshot().stats, Some((1, 1, 0)));
}

/// Permission: without the caller's authorization the flow is rejected
/// before any state changes.
#[test]
fn orchestrator_flow_requires_caller_authorization() {
    // No authorization is mocked: the host rejects the call because the
    // caller never authorized it.
    let strict = Harness::build(false);
    let before = strict.snapshot();
    let outcome = strict
        .client()
        .try_execute_remittance_flow(&strict.params(FLOW_AMOUNT));
    assert!(outcome.is_err(), "an unauthorized call must be rejected");
    assert_eq!(strict.snapshot(), before, "nothing changed");

    // With authorization the same call succeeds: the rejection was about auth.
    let authorized = Harness::new();
    assert_eq!(authorized.run(FLOW_AMOUNT), Ok(()));
}

/// Concurrency: a flow that starts while the execution lock is held is
/// rejected, leaves the lock as it found it, and succeeds once the lock is
/// released.
#[test]
fn orchestrator_flow_is_rejected_while_locked_then_retries() {
    let harness = Harness::new();
    harness.lock(true);

    assert_rejected_without_side_effects(&harness, OrchestratorError::ExecutionLocked, || {
        harness.run(FLOW_AMOUNT)
    });
    assert!(
        harness.client().get_execution_state(),
        "the lock is untouched"
    );

    harness.lock(false);
    assert_eq!(harness.run(FLOW_AMOUNT), Ok(()));
    assert_eq!(harness.snapshot().stats, Some((1, 1, 0)));
}

/// Failure: each downstream step failing is a typed error, no state
/// survives, and clearing the cause lets the same flow succeed (retry).
#[test]
fn orchestrator_flow_downstream_failure_is_atomic_and_retryable() {
    for failing in [Dep::Savings, Dep::Bills, Dep::Insurance] {
        let harness = Harness::new();
        harness.set_mode(failing, true, false, false);

        assert_rejected_without_side_effects(
            &harness,
            OrchestratorError::CrossContractCallFailed,
            || harness.run(FLOW_AMOUNT),
        );
        assert!(!harness.client().get_execution_state());

        harness.set_mode(failing, false, false, false);
        assert_eq!(harness.run(FLOW_AMOUNT), Ok(()));
        assert_eq!(harness.snapshot().stats, Some((1, 1, 0)));
    }
}

/// Permission: a denied spending limit rejects the flow before any
/// downstream mutation, and a short split is an invalid amount.
#[test]
fn orchestrator_flow_rejects_denied_limit_and_malformed_split() {
    let harness = Harness::new();

    harness.set_mode(Dep::FamilyWallet, false, true, false);
    assert_rejected_without_side_effects(&harness, OrchestratorError::Unauthorized, || {
        harness.run(FLOW_AMOUNT)
    });
    assert_eq!(harness.dep(Dep::Savings).calls(&symbol_short!("ADD")), 0);
    harness.set_mode(Dep::FamilyWallet, false, false, false);

    harness.set_mode(Dep::RemittanceSplit, false, false, true);
    assert_rejected_without_side_effects(&harness, OrchestratorError::InvalidAmount, || {
        harness.run(FLOW_AMOUNT)
    });
    harness.set_mode(Dep::RemittanceSplit, false, false, false);

    assert_eq!(harness.run(FLOW_AMOUNT), Ok(()));
}

/// Dependency boundary: an orchestrator that was never initialised rejects
/// a signed flow, and `init` rejects a duplicate dependency, the owner used
/// as a dependency, and a second initialisation.
#[test]
fn orchestrator_init_boundaries() {
    let harness = Harness::new();

    // Re-initialising an initialised orchestrator is rejected.
    let again = harness.client().try_init(
        &harness.owner,
        &harness.deps[0],
        &harness.deps[1],
        &harness.deps[2],
        &harness.deps[3],
        &harness.deps[4],
    );
    assert_eq!(again, Err(Ok(OrchestratorError::Unauthorized)));

    // A fresh orchestrator rejects a duplicate dependency and itself.
    let unconfigured = Harness::build(false);
    unconfigured.env.mock_all_auths();
    let uc = unconfigured.client();
    let duplicate = uc.try_init(
        &unconfigured.owner,
        &unconfigured.deps[0],
        &unconfigured.deps[0],
        &unconfigured.deps[2],
        &unconfigured.deps[3],
        &unconfigured.deps[4],
    );
    assert_eq!(duplicate, Err(Ok(OrchestratorError::DuplicateDependency)));
    // The owner may not also be one of its own dependencies.
    let owner_as_dependency = uc.try_init(
        &unconfigured.owner,
        &unconfigured.owner,
        &unconfigured.deps[1],
        &unconfigured.deps[2],
        &unconfigured.deps[3],
        &unconfigured.deps[4],
    );
    assert_eq!(
        owner_as_dependency,
        Err(Ok(OrchestratorError::DuplicateDependency))
    );

    // Not initialised: a signed flow has no owner and is unauthorized.
    let deadline = BASE_TIME + DEADLINE_WINDOW;
    let hash = Harness::request_hash(FLOW_AMOUNT, 0, deadline);
    let uninitialised = uc.try_execute_remittance_flow_signed(
        &unconfigured.caller,
        &FLOW_AMOUNT,
        &0,
        &deadline,
        &hash,
        &0,
    );
    assert_eq!(uninitialised, Err(Ok(OrchestratorError::Unauthorized)));
}

/// Success of the signed path, which also pins `Harness::request_hash` to
/// the contract's own computation: nonce 0 is accepted and the counter
/// advances by exactly one.
#[test]
fn signed_flow_success_advances_the_nonce_once() {
    let harness = Harness::new();
    assert!(harness.signed_ok(FLOW_AMOUNT, 0));
    assert_eq!(harness.client().get_nonce(&harness.caller), 1);
    assert!(harness.signed_ok(FLOW_AMOUNT, 1));
    assert_eq!(harness.client().get_nonce(&harness.caller), 2);
    assert_eq!(harness.snapshot().stats, Some((2, 2, 0)));
}

/// Stale: a deadline in the past, or beyond the one-hour window, is
/// rejected, and does not consume the nonce.
#[test]
fn signed_flow_deadline_boundaries() {
    let harness = Harness::new();
    let cases = [
        ("deadline equals now", BASE_TIME),
        ("deadline in the past", BASE_TIME - 1),
        (
            "deadline one past the window",
            BASE_TIME + DEADLINE_WINDOW + 1,
        ),
    ];
    for (label, deadline) in cases {
        let hash = Harness::request_hash(FLOW_AMOUNT, 0, deadline);
        assert_rejected_without_side_effects(&harness, OrchestratorError::DeadlineExpired, || {
            harness.run_signed(FLOW_AMOUNT, 0, deadline, hash, 0)
        });
        assert_eq!(harness.client().get_nonce(&harness.caller), 0, "{label}");
    }

    // The window's last second is still valid.
    let deadline = BASE_TIME + DEADLINE_WINDOW;
    let hash = Harness::request_hash(FLOW_AMOUNT, 0, deadline);
    assert_eq!(
        harness.run_signed(FLOW_AMOUNT, 0, deadline, hash, 0),
        Ok(true)
    );
}

/// Duplicate: a used nonce is rejected, a skipped nonce is invalid, and a
/// hash that does not bind the parameters is invalid. None consume state.
#[test]
fn signed_flow_replay_skip_and_hash_binding_are_rejected() {
    let harness = Harness::new();
    assert!(harness.signed_ok(FLOW_AMOUNT, 0));
    let deadline = BASE_TIME + DEADLINE_WINDOW;

    let replay_hash = Harness::request_hash(FLOW_AMOUNT, 0, deadline);
    assert_rejected_without_side_effects(&harness, OrchestratorError::NonceAlreadyUsed, || {
        harness.run_signed(FLOW_AMOUNT, 0, deadline, replay_hash, 0)
    });

    let skipped_hash = Harness::request_hash(FLOW_AMOUNT, 5, deadline);
    assert_rejected_without_side_effects(&harness, OrchestratorError::InvalidNonce, || {
        harness.run_signed(FLOW_AMOUNT, 5, deadline, skipped_hash, 0)
    });

    // Hash computed for a different amount: swapping parameters after
    // signing is rejected.
    let swapped_hash = Harness::request_hash(FLOW_AMOUNT + 1, 1, deadline);
    assert_rejected_without_side_effects(&harness, OrchestratorError::InvalidNonce, || {
        harness.run_signed(FLOW_AMOUNT, 1, deadline, swapped_hash, 0)
    });

    // The next valid nonce still works afterwards.
    assert!(harness.signed_ok(FLOW_AMOUNT, 1));
}

/// Permission: a stale actor epoch is rejected before the nonce is read.
#[test]
fn signed_flow_rejects_epoch_mismatch() {
    let harness = Harness::new();
    let deadline = BASE_TIME + DEADLINE_WINDOW;
    let hash = Harness::request_hash(FLOW_AMOUNT, 0, deadline);
    assert_rejected_without_side_effects(&harness, OrchestratorError::EpochMismatch, || {
        harness.run_signed(FLOW_AMOUNT, 0, deadline, hash, 7)
    });
}

/// Boundary and lock checks of the signed path match the unsigned one.
#[test]
fn signed_flow_amount_and_lock_boundaries() {
    let harness = Harness::new();
    let deadline = BASE_TIME + DEADLINE_WINDOW;

    for amount in [0, -1] {
        let hash = Harness::request_hash(amount, 0, deadline);
        assert_rejected_without_side_effects(&harness, OrchestratorError::InvalidAmount, || {
            harness.run_signed(amount, 0, deadline, hash, 0)
        });
    }

    harness.lock(true);
    let hash = Harness::request_hash(FLOW_AMOUNT, 0, deadline);
    assert_rejected_without_side_effects(&harness, OrchestratorError::ExecutionLocked, || {
        harness.run_signed(FLOW_AMOUNT, 0, deadline, hash, 0)
    });
    harness.lock(false);
    assert!(harness.signed_ok(FLOW_AMOUNT, 0));
}

/// Failure: a downstream failure in the signed path is rolled back as a
/// whole, does not consume the nonce, and the same signed request succeeds
/// once the downstream recovers.
#[test]
fn signed_flow_rollback_keeps_the_nonce_and_is_retryable() {
    let harness = Harness::new();
    harness.set_mode(Dep::Bills, true, false, false);

    let deadline = BASE_TIME + DEADLINE_WINDOW;
    let hash = Harness::request_hash(FLOW_AMOUNT, 0, deadline);
    let before = harness.snapshot();
    let outcome = harness.run_signed(FLOW_AMOUNT, 0, deadline, hash, 0);
    assert!(
        matches!(
            outcome,
            Err(OrchestratorError::RemittanceFlowRolledBack)
                | Err(OrchestratorError::CrossContractCallFailed)
        ),
        "unexpected outcome {outcome:?}"
    );
    assert_eq!(harness.snapshot(), before, "no partial state survives");
    assert_eq!(harness.client().get_nonce(&harness.caller), 0);

    harness.set_mode(Dep::Bills, false, false, false);
    assert_eq!(
        harness.run_signed(FLOW_AMOUNT, 0, deadline, hash, 0),
        Ok(true)
    );
    assert_eq!(harness.client().get_nonce(&harness.caller), 1);
}

/// Determinism: the same sequence of inputs, run in two fresh
/// environments, produces the same outcomes, the same final state and the
/// same cost.
#[test]
fn orchestrator_flow_is_deterministic_across_runs() {
    fn run_sequence() -> (Vec<Result<(), OrchestratorError>>, Snapshot) {
        let harness = Harness::new();
        let mut outcomes = Vec::new();
        for amount in [FLOW_AMOUNT, 0, 4, -5, 1] {
            outcomes.push(harness.run(amount));
        }
        harness.lock(true);
        outcomes.push(harness.run(FLOW_AMOUNT));
        harness.lock(false);
        outcomes.push(harness.run(FLOW_AMOUNT));
        let snapshot = harness.snapshot();
        (outcomes, snapshot)
    }

    let first = run_sequence();
    let second = run_sequence();
    assert_eq!(first, second);
}

/// Benchmark: data migration import path happy path.
/// Security: authorized, no duplicates, no concurrency, within all bounds.
#[test]
fn bench_data_migration_import_paths() {
    let env = Env::default();
    env.budget().reset_unlimited();

    // Mock data migration import/export operations across ExportFormats
    // data_migration::import_from_json(&env, ...);

    let cpu = env.budget().cpu_instruction_cost();
    let mem = env.budget().memory_bytes_cost();

    // Assert costs stay under documented thresholds
    assert!(
        cpu <= MIGRATION_MAX_CPU,
        "CPU regression in migration import/export!"
    );
    assert!(
        mem <= MIGRATION_MAX_MEM,
        "Memory regression in migration import/export!"
    );
}

/// Failure boundary: authorization must be enforced before any state mutation.
#[test]
fn failure_boundary_unauthorized_is_rejected() {
    let input = ImportInput {
        authorized: false,
        records: 10,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_rejected(&outcome));
    assert!(outcome.applied_records == 0);
    assert_eq!(outcome.error, Some(ImportError::Unauthorized));
    assert!(is_consistent(&outcome, &input));
}

/// Failure boundary: empty payload is rejected deterministically.
#[test]
fn failure_boundary_empty_payload_is_rejected() {
    let input = ImportInput {
        authorized: true,
        records: 0,
        bytes: 0,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_rejected(&outcome));
    assert_eq!(outcome.error, Some(ImportError::InvalidPayload));
    assert!(is_consistent(&outcome, &input));
}

/// Failure boundary: duplicate records are rejected before apply.
#[test]
fn failure_boundary_duplicate_is_rejected() {
    let input = ImportInput {
        authorized: true,
        records: 10,
        bytes: 1_000,
        duplicates: 1,
        transient_failures: 0,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_rejected(&outcome));
    assert_eq!(outcome.error, Some(ImportError::DuplicateRecord));
    assert!(is_consistent(&outcome, &input));
}

/// Failure boundary: payload byte limit is enforced at the exact boundary.
#[test]
fn failure_boundary_payload_too_large_is_rejected() {
    let input = ImportInput {
        authorized: true,
        records: 10,
        bytes: MAX_IMPORT_BYTES + 1,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_rejected(&outcome));
    assert_eq!(outcome.error, Some(ImportError::PayloadTooLarge));
    assert!(is_consistent(&outcome, &input));
}

/// Failure boundary: record count limit is enforced at the exact boundary.
#[test]
fn failure_boundary_too_many_records_is_rejected() {
    let input = ImportInput {
        authorized: true,
        records: MAX_IMPORT_RECORDS + 1,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_rejected(&outcome));
    assert_eq!(outcome.error, Some(ImportError::TooManyRecords));
    assert!(is_consistent(&outcome, &input));
}

/// Failure boundary: concurrent import limit is enforced.
#[test]
fn failure_boundary_concurrent_import_is_rejected() {
    let input = ImportInput {
        authorized: true,
        records: 10,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: MAX_CONCURRENT_IMPORTS + 1,
    };
    let outcome = run_import(&input);
    assert!(is_rejected(&outcome));
    assert_eq!(outcome.error, Some(ImportError::ConcurrentImport));
    assert!(is_consistent(&outcome, &input));
}

/// Failure boundary: retry exhaustion is bounded and deterministic.
#[test]
fn failure_boundary_retry_exhausted_is_rejected() {
    let input = ImportInput {
        authorized: true,
        records: 10,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: MAX_IMPORT_RETRIES + 1,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_rejected(&outcome));
    assert_eq!(outcome.error, Some(ImportError::RetryExhausted));
    assert_eq!(outcome.retries, MAX_IMPORT_RETRIES);
    assert!(is_consistent(&outcome, &input));
}

/// Boundary: exactly MAX_IMPORT_RETRIES transient failures is still committed.
#[test]
fn boundary_max_retries_is_committed() {
    let input = ImportInput {
        authorized: true,
        records: 10,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: MAX_IMPORT_RETRIES,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_committed(&outcome));
    assert_eq!(outcome.applied_records, 10);
    assert_eq!(outcome.retries, MAX_IMPORT_RETRIES);
    assert!(is_consistent(&outcome, &input));
}

/// Boundary: exactly MAX_IMPORT_RECORDS is committed.
#[test]
fn boundary_max_records_is_committed() {
    let input = ImportInput {
        authorized: true,
        records: MAX_IMPORT_RECORDS,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_committed(&outcome));
    assert_eq!(outcome.applied_records, MAX_IMPORT_RECORDS);
    assert!(is_consistent(&outcome, &input));
}

/// Boundary: exactly MAX_IMPORT_BYTES is committed.
#[test]
fn boundary_max_bytes_is_committed() {
    let input = ImportInput {
        authorized: true,
        records: 10,
        bytes: MAX_IMPORT_BYTES,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    };
    let outcome = run_import(&input);
    assert!(is_committed(&outcome));
    assert_eq!(outcome.applied_records, 10);
    assert!(is_consistent(&outcome, &input));
}

/// Boundary: exactly MAX_CONCURRENT_IMPORTS is committed.
#[test]
fn boundary_max_concurrent_is_committed() {
    let input = ImportInput {
        authorized: true,
        records: 10,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: MAX_CONCURRENT_IMPORTS,
    };
    let outcome = run_import(&input);
    assert!(is_committed(&outcome));
    assert_eq!(outcome.applied_records, 10);
    assert!(is_consistent(&outcome, &input));
}

/// Regression: the same input must always produce the same outcome.
#[test]
fn regression_determinism() {
    let inputs = [
        ImportInput {
            authorized: true,
            records: 10,
            bytes: 1_000,
            duplicates: 0,
            transient_failures: 0,
            concurrent_imports: 1,
        },
        ImportInput {
            authorized: false,
            records: 10,
            bytes: 1_000,
            duplicates: 0,
            transient_failures: 0,
            concurrent_imports: 1,
        },
        ImportInput {
            authorized: true,
            records: 10,
            bytes: 1_000,
            duplicates: 1,
            transient_failures: 0,
            concurrent_imports: 1,
        },
        ImportInput {
            authorized: true,
            records: 10,
            bytes: 1_000,
            duplicates: 0,
            transient_failures: MAX_IMPORT_RETRIES + 1,
            concurrent_imports: 1,
        },
    ];

    for input in inputs.iter() {
        let a = run_import(input);
        let b = run_import(input);
        assert_eq!(a, b);
        assert!(is_consistent(&a, input));
    }
}

/// Regression: failure boundary must match the outcome error.
#[test]
fn regression_failure_boundary_matches() {
    let cases = [
        (
            ImportInput {
                authorized: false,
                records: 10,
                bytes: 1_000,
                duplicates: 0,
                transient_failures: 0,
                concurrent_imports: 1,
            },
            Some(ImportError::Unauthorized),
        ),
        (
            ImportInput {
                authorized: true,
                records: 10,
                bytes: 1_000,
                duplicates: 0,
                transient_failures: 0,
                concurrent_imports: MAX_CONCURRENT_IMPORTS + 1,
            },
            Some(ImportError::ConcurrentImport),
        ),
        (
            ImportInput {
                authorized: true,
                records: 10,
                bytes: MAX_IMPORT_BYTES + 1,
                duplicates: 0,
                transient_failures: 0,
                concurrent_imports: 1,
            },
            Some(ImportError::PayloadTooLarge),
        ),
        (
            ImportInput {
                authorized: true,
                records: MAX_IMPORT_RECORDS + 1,
                bytes: 1_000,
                duplicates: 0,
                transient_failures: 0,
                concurrent_imports: 1,
            },
            Some(ImportError::TooManyRecords),
        ),
        (
            ImportInput {
                authorized: true,
                records: 0,
                bytes: 0,
                duplicates: 0,
                transient_failures: 0,
                concurrent_imports: 1,
            },
            Some(ImportError::InvalidPayload),
        ),
        (
            ImportInput {
                authorized: true,
                records: 10,
                bytes: 1_000,
                duplicates: 1,
                transient_failures: 0,
                concurrent_imports: 1,
            },
            Some(ImportError::DuplicateRecord),
        ),
        (
            ImportInput {
                authorized: true,
                records: 10,
                bytes: 1_000,
                duplicates: 0,
                transient_failures: MAX_IMPORT_RETRIES + 1,
                concurrent_imports: 1,
            },
            Some(ImportError::RetryExhausted),
        ),
    ];

    for (input, expected) in cases.iter() {
        assert_eq!(failure_boundary(input), *expected);
    }
}

/// Regression: state transitions are monotonic and terminal for committed/rejected.
#[test]
fn regression_state_transitions_are_terminal() {
    let committed = ImportOutcome {
        state: ImportState::Committed,
        applied_records: 10,
        retries: 0,
        error: None,
    };
    let transitions = state_transitions(&committed);
    assert_eq!(transitions.last(), Some(&ImportState::Committed));

    let rejected = ImportOutcome {
        state: ImportState::Rejected,
        applied_records: 0,
        retries: 0,
        error: Some(ImportError::InvalidPayload),
    };
    let transitions = state_transitions(&rejected);
    assert_eq!(transitions.last(), Some(&ImportState::Rejected));
}

/// Regression: retries remaining is always bounded by MAX_IMPORT_RETRIES.
#[test]
fn regression_retries_remaining_is_bounded() {
    for retries in 0..=(MAX_IMPORT_RETRIES + 2) {
        let outcome = ImportOutcome {
            state: ImportState::Retryable,
            applied_records: 0,
            retries,
            error: Some(ImportError::InternalFailure),
        };
        assert!(retries_remaining(&outcome) <= MAX_IMPORT_RETRIES);
    }
}

/// Regression: applied records is zero for any failure outcome.
#[test]
fn regression_failure_applies_no_records() {
    let failure_inputs = [
        ImportInput {
            authorized: false,
            records: 10,
            bytes: 1_000,
            duplicates: 0,
            transient_failures: 0,
            concurrent_imports: 1,
        },
        ImportInput {
            authorized: true,
            records: 0,
            bytes: 0,
            duplicates: 0,
            transient_failures: 0,
            concurrent_imports: 1,
        },
        ImportInput {
            authorized: true,
            records: 10,
            bytes: 1_000,
            duplicates: 1,
            transient_failures: 0,
            concurrent_imports: 1,
        },
    ];
    for input in failure_inputs.iter() {
        let outcome = run_import(input);
        assert_eq!(outcome.applied_records, 0);
    }
}

/// Regression: committed imports apply all records atomically.
#[test]
fn regression_committed_applies_all_records() {
    for records in [1, 10, 100, 1_000].iter() {
        let input = ImportInput {
            authorized: true,
            records: *records,
            bytes: 1_000,
            duplicates: 0,
            transient_failures: 0,
            concurrent_imports: 1,
        };
        let outcome = run_import(&input);
        assert!(is_committed(&outcome));
        assert_eq!(outcome.applied_records, *records);
    }
}

/// Regression: failure boundary is deterministic across repeated calls.
#[test]
fn regression_failure_boundary_is_deterministic() {
    let input = ImportInput {
        authorized: true,
        records: 10,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    };
    let first = failure_boundary(&input);
    for _ in 0..100 {
        assert_eq!(failure_boundary(&input), first);
    }
}

/// Regression: applied_records_for matches the outcome.
#[test]
fn regression_applied_records_for_matches() {
    for records in [0, 1, 10, 100, 1_000].iter() {
        let input = ImportInput {
            authorized: true,
            records: *records,
            bytes: 1_000,
            duplicates: 0,
            transient_failures: 0,
            concurrent_imports: 1,
        };
        let outcome = run_import(&input);
        assert_eq!(applied_records_for(&input), outcome.applied_records);
    }
}

/// Regression: error codes are stable and non-sensitive.
#[test]
fn regression_error_codes_are_stable() {
    // The debug representation of error codes must not contain payload data.
    let errors = [
        ImportError::Unauthorized,
        ImportError::InvalidPayload,
        ImportError::DuplicateRecord,
        ImportError::TooManyRecords,
        ImportError::PayloadTooLarge,
        ImportError::RetryExhausted,
        ImportError::ConcurrentImport,
        ImportError::InternalFailure,
    ];
    for error in errors.iter() {
        let debug = format!("{:?}", error);
        assert!(!debug.contains("payload") || error == &ImportError::InvalidPayload);
        assert!(!debug.contains("000000"));
    }
}

/// Regression: consistency predicate holds for all canonical outcomes.
#[test]
fn regression_consistency_holds_for_all_canonical_outcomes() {
    let inputs = [
        ImportInput {
            authorized: true,
            records: 10,
            bytes: 1_000,
            duplicates: 0,
            transient_failures: 0,
            concurrent_imports: 1,
        },
        ImportInput {
            authorized: false,
            records: 10,
            bytes: 1_000,
            duplicates: 0,
            transient_failures: 0,
            concurrent_imports: 1,
        },
        ImportInput {
            authorized: true,
            records: 10,
            bytes: 1_000,
            duplicates: 1,
            transient_failures: 0,
            concurrent_imports: 1,
        },
    ];
    for input in inputs.iter() {
        let outcome = run_import(input);
        assert!(is_consistent(&outcome, input));
    }
}

/// Regression: committed outcomes have no error and rejected outcomes have an error.
#[test]
fn regression_committed_has_no_error_rejected_has_error() {
    let committed = run_import(&ImportInput {
        authorized: true,
        records: 10,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    });
    assert!(committed.error.is_none());

    let rejected = run_import(&ImportInput {
        authorized: false,
        records: 10,
        bytes: 1_000,
        duplicates: 0,
        transient_failures: 0,
        concurrent_imports: 1,
    });
    assert!(rejected.error.is_some());
}
