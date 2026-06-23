use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;

pub const SEMANTIC_TRACE_SCHEMA: &str = "carbon-scheduler.semantic-trace.v0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    #[serde(default = "default_nested_tasklets")]
    pub nested_tasklets: bool,
    #[serde(default)]
    pub channel_callbacks: bool,
    #[serde(default)]
    pub channels: Vec<ChannelSpec>,
    #[serde(default)]
    pub tasklets: Vec<TaskletSpec>,
    pub entrypoint: Entrypoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelSpec {
    pub id: String,
    #[serde(default = "default_channel_preference")]
    pub preference: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskletSpec {
    pub id: String,
    #[serde(default = "default_initially_scheduled")]
    pub initially_scheduled: bool,
    #[serde(default = "default_initially_bound")]
    pub initially_bound: bool,
    #[serde(default)]
    pub body: Vec<Operation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Entrypoint {
    RunScheduler,
    RunSchedulerN { count: usize },
    RunSchedulerWithTimeout { timeout_ns: i64 },
    RunTasklet { tasklet: String },
    RunSchedulerThenSend { channel: String, value: Value },
    RunSchedulerThenSendThenRun { channel: String, value: Value },
    Receive { actor: String, channel: String },
    Script { steps: Vec<MainStep> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Operation {
    Append {
        target: String,
        value: Value,
    },
    Spawn {
        tasklet: String,
    },
    RunTasklet {
        tasklet: String,
    },
    Schedule,
    ScheduleRemove,
    SwitchTrap {
        delta: i64,
    },
    Send {
        channel: String,
        value: Value,
    },
    SendException {
        channel: String,
        exception: String,
        #[serde(default)]
        args: Value,
    },
    SendThrow {
        channel: String,
        exception: String,
        #[serde(default)]
        value: Value,
        #[serde(default)]
        traceback: Option<String>,
    },
    SetBlockTrap {
        value: bool,
    },
    Receive {
        channel: String,
        #[serde(default)]
        bind: Option<String>,
    },
    Close {
        channel: String,
    },
    Open {
        channel: String,
    },
    Clear {
        channel: String,
        #[serde(default)]
        pending: bool,
    },
    QueueFront {
        channel: String,
        #[serde(default)]
        target: Option<String>,
    },
    RemoveTasklet {
        tasklet: String,
    },
    InsertTasklet {
        tasklet: String,
    },
    KillTasklet {
        tasklet: String,
        #[serde(default)]
        pending: bool,
    },
    SwitchTasklet {
        tasklet: String,
    },
    BindTasklet {
        tasklet: String,
        #[serde(default)]
        body: Option<Vec<Operation>>,
        #[serde(default)]
        args_bound: bool,
    },
    SetupTasklet {
        tasklet: String,
    },
    UnbindTasklet {
        tasklet: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MainStep {
    Schedule,
    ScheduleRemove,
    RunScheduler,
    RunSchedulerN {
        count: usize,
    },
    RunSchedulerWithTimeout {
        timeout_ns: i64,
    },
    Send {
        channel: String,
        value: Value,
    },
    SendException {
        channel: String,
        exception: String,
        #[serde(default)]
        args: Value,
    },
    SendThrow {
        channel: String,
        exception: String,
        #[serde(default)]
        value: Value,
        #[serde(default)]
        traceback: Option<String>,
    },
    SetBlockTrap {
        value: bool,
    },
    SwitchTrap {
        delta: i64,
    },
    Receive {
        channel: String,
        #[serde(default)]
        bind: Option<String>,
    },
    Close {
        channel: String,
    },
    Open {
        channel: String,
    },
    Clear {
        channel: String,
        #[serde(default)]
        pending: bool,
    },
    Remove {
        tasklet: String,
    },
    Insert {
        tasklet: String,
    },
    Kill {
        tasklet: String,
        #[serde(default)]
        pending: bool,
    },
    Switch {
        tasklet: String,
    },
    RunTasklet {
        tasklet: String,
    },
    Bind {
        tasklet: String,
        #[serde(default)]
        body: Option<Vec<Operation>>,
        #[serde(default)]
        args_bound: bool,
    },
    Setup {
        tasklet: String,
    },
    Unbind {
        tasklet: String,
    },
    RaiseException {
        tasklet: String,
        exception: String,
    },
    QueueFront {
        channel: String,
        #[serde(default)]
        target: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceRun {
    pub schema: &'static str,
    pub events: Vec<Value>,
    pub final_state: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    DuplicateTasklet(String),
    DuplicateChannel(String),
    MissingTasklet(String),
    MissingChannel(String),
    InvalidTaskletOperation {
        tasklet: String,
        operation: &'static str,
        reason: &'static str,
    },
    UnsupportedMainOperation(String),
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTasklet(id) => write!(f, "duplicate tasklet id: {id}"),
            Self::DuplicateChannel(id) => write!(f, "duplicate channel id: {id}"),
            Self::MissingTasklet(id) => write!(f, "missing tasklet: {id}"),
            Self::MissingChannel(id) => write!(f, "missing channel: {id}"),
            Self::InvalidTaskletOperation {
                tasklet,
                operation,
                reason,
            } => write!(f, "cannot {operation} tasklet {tasklet}: {reason}"),
            Self::UnsupportedMainOperation(op) => write!(f, "unsupported main operation: {op}"),
        }
    }
}

impl Error for SchedulerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CoreTaskletId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CoreChannelId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CoreRunQueueId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoreTaskletLifecycle {
    Runnable,
    Blocked,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoreChannelDirection {
    Send,
    Receive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreBlockedOnChannel {
    pub channel: CoreChannelId,
    pub direction: CoreChannelDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreTaskletSnapshot {
    pub lifecycle: CoreTaskletLifecycle,
    pub blocked_on: Option<CoreBlockedOnChannel>,
    pub block_trap: bool,
    pub alive: bool,
    pub scheduled: bool,
    pub paused: bool,
    pub times_switched_to: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreChannelSnapshot {
    pub preference: i64,
    pub closing: bool,
    pub closed: bool,
    pub balance: i64,
    pub blocked_senders: Vec<CoreTaskletId>,
    pub blocked_receivers: Vec<CoreTaskletId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreChannelOperationResult {
    Blocked {
        tasklet: CoreTaskletId,
        channel: CoreChannelId,
        direction: CoreChannelDirection,
        balance: i64,
    },
    Matched {
        sender: CoreTaskletId,
        receiver: CoreTaskletId,
        channel: CoreChannelId,
        preferred: CoreTaskletId,
        peer_runs_immediately: bool,
        balance: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreSchedulerHandleError {
    MissingTasklet(CoreTaskletId),
    MissingChannel(CoreChannelId),
    MissingRunQueue(CoreRunQueueId),
    ClosedChannel(CoreChannelId),
    BlockingDisallowed {
        tasklet: CoreTaskletId,
        channel: CoreChannelId,
        direction: CoreChannelDirection,
    },
}

impl fmt::Display for CoreSchedulerHandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingTasklet(tasklet) => write!(f, "missing core tasklet: {tasklet:?}"),
            Self::MissingChannel(channel) => write!(f, "missing core channel: {channel:?}"),
            Self::MissingRunQueue(queue) => write!(f, "missing core run queue: {queue:?}"),
            Self::ClosedChannel(channel) => {
                write!(f, "operation on closed core channel: {channel:?}")
            }
            Self::BlockingDisallowed {
                tasklet,
                channel,
                direction,
            } => write!(
                f,
                "core tasklet {tasklet:?} cannot block on {direction:?} for channel {channel:?}"
            ),
        }
    }
}

impl Error for CoreSchedulerHandleError {}

#[derive(Debug, Clone)]
struct CoreTaskletState {
    lifecycle: CoreTaskletLifecycle,
    blocked_on: Option<CoreBlockedOnChannel>,
    block_trap: bool,
    alive: bool,
    scheduled: bool,
    run_queue: Option<CoreRunQueueId>,
    paused: bool,
    times_switched_to: u64,
}

#[derive(Debug, Clone)]
struct CoreChannelState {
    preference: i64,
    closing: bool,
    closed: bool,
    blocked_senders: VecDeque<CoreTaskletId>,
    blocked_receivers: VecDeque<CoreTaskletId>,
}

#[derive(Debug, Clone, Default)]
struct CoreRunQueueState {
    runnable: VecDeque<CoreTaskletId>,
}

#[derive(Debug, Default, Clone)]
pub struct CoreScheduler {
    next_tasklet: u64,
    next_channel: u64,
    next_run_queue: u64,
    tasklets: BTreeMap<CoreTaskletId, CoreTaskletState>,
    channels: BTreeMap<CoreChannelId, CoreChannelState>,
    run_queues: BTreeMap<CoreRunQueueId, CoreRunQueueState>,
}

impl CoreScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_tasklet(&mut self) -> CoreTaskletId {
        let id = CoreTaskletId(self.next_tasklet);
        self.next_tasklet += 1;
        self.tasklets.insert(
            id,
            CoreTaskletState {
                lifecycle: CoreTaskletLifecycle::Runnable,
                blocked_on: None,
                block_trap: false,
                alive: true,
                scheduled: false,
                run_queue: None,
                paused: false,
                times_switched_to: 0,
            },
        );
        id
    }

    pub fn create_channel(&mut self, preference: i64) -> CoreChannelId {
        let id = CoreChannelId(self.next_channel);
        self.next_channel += 1;
        self.channels.insert(
            id,
            CoreChannelState {
                preference: preference.clamp(-1, 1),
                closing: false,
                closed: false,
                blocked_senders: VecDeque::new(),
                blocked_receivers: VecDeque::new(),
            },
        );
        id
    }

    pub fn create_run_queue(&mut self) -> CoreRunQueueId {
        let id = CoreRunQueueId(self.next_run_queue);
        self.next_run_queue += 1;
        self.run_queues.insert(id, CoreRunQueueState::default());
        id
    }

    pub fn set_tasklet_block_trap(
        &mut self,
        tasklet: CoreTaskletId,
        value: bool,
    ) -> Result<(), CoreSchedulerHandleError> {
        self.tasklet_mut(tasklet)?.block_trap = value;
        Ok(())
    }

    pub fn tasklet_lifecycle(
        &self,
        tasklet: CoreTaskletId,
    ) -> Result<CoreTaskletLifecycle, CoreSchedulerHandleError> {
        Ok(self.tasklet(tasklet)?.lifecycle)
    }

    pub fn tasklet_blocked_on(
        &self,
        tasklet: CoreTaskletId,
    ) -> Result<Option<CoreBlockedOnChannel>, CoreSchedulerHandleError> {
        Ok(self.tasklet(tasklet)?.blocked_on)
    }

    pub fn tasklet_snapshot(
        &self,
        tasklet: CoreTaskletId,
    ) -> Result<CoreTaskletSnapshot, CoreSchedulerHandleError> {
        Ok(self.tasklet(tasklet)?.snapshot())
    }

    pub fn update_tasklet_runtime_state(
        &mut self,
        tasklet: CoreTaskletId,
        alive: bool,
        paused: bool,
        times_switched_to: u64,
    ) -> Result<(), CoreSchedulerHandleError> {
        self.ensure_tasklet(tasklet)?;
        if !alive {
            self.remove_tasklet_from_run_queues(tasklet);
        }
        let tasklet_state = self.tasklet_mut(tasklet)?;
        tasklet_state.alive = alive;
        tasklet_state.paused = paused;
        tasklet_state.times_switched_to = times_switched_to;
        if !alive {
            tasklet_state.lifecycle = CoreTaskletLifecycle::Complete;
            tasklet_state.blocked_on = None;
            tasklet_state.scheduled = false;
        } else if tasklet_state.blocked_on.is_none() {
            tasklet_state.lifecycle = CoreTaskletLifecycle::Runnable;
        }
        Ok(())
    }

    pub fn pause_tasklet(
        &mut self,
        tasklet: CoreTaskletId,
    ) -> Result<(), CoreSchedulerHandleError> {
        self.ensure_tasklet(tasklet)?;
        self.remove_tasklet_from_run_queues(tasklet);
        let tasklet_state = self.tasklet_mut(tasklet)?;
        tasklet_state.alive = true;
        tasklet_state.scheduled = false;
        tasklet_state.paused = true;
        if tasklet_state.blocked_on.is_none() {
            tasklet_state.lifecycle = CoreTaskletLifecycle::Runnable;
        }
        Ok(())
    }

    pub fn resume_tasklet(
        &mut self,
        tasklet: CoreTaskletId,
    ) -> Result<(), CoreSchedulerHandleError> {
        let tasklet_state = self.tasklet_mut(tasklet)?;
        tasklet_state.alive = true;
        tasklet_state.paused = false;
        if tasklet_state.blocked_on.is_none() {
            tasklet_state.lifecycle = CoreTaskletLifecycle::Runnable;
        }
        Ok(())
    }

    pub fn assign_tasklet_run_queue(
        &mut self,
        tasklet: CoreTaskletId,
        queue: CoreRunQueueId,
    ) -> Result<(), CoreSchedulerHandleError> {
        self.ensure_tasklet(tasklet)?;
        self.ensure_run_queue(queue)?;
        self.tasklet_mut(tasklet)?.run_queue = Some(queue);
        Ok(())
    }

    pub fn schedule_tasklet_back(
        &mut self,
        queue: CoreRunQueueId,
        tasklet: CoreTaskletId,
    ) -> Result<(), CoreSchedulerHandleError> {
        self.ensure_tasklet(tasklet)?;
        self.ensure_run_queue(queue)?;
        self.remove_tasklet_from_run_queues(tasklet);
        self.run_queue_mut(queue)?.runnable.push_back(tasklet);
        let tasklet_state = self.tasklet_mut(tasklet)?;
        tasklet_state.run_queue = Some(queue);
        tasklet_state.scheduled = true;
        tasklet_state.alive = true;
        tasklet_state.paused = false;
        if tasklet_state.blocked_on.is_none() {
            tasklet_state.lifecycle = CoreTaskletLifecycle::Runnable;
        }
        Ok(())
    }

    pub fn remove_runnable_tasklet(
        &mut self,
        tasklet: CoreTaskletId,
    ) -> Result<(), CoreSchedulerHandleError> {
        self.ensure_tasklet(tasklet)?;
        self.remove_tasklet_from_run_queues(tasklet);
        self.tasklet_mut(tasklet)?.scheduled = false;
        Ok(())
    }

    pub fn pop_next_runnable_tasklet(
        &mut self,
        queue: CoreRunQueueId,
    ) -> Result<Option<CoreTaskletId>, CoreSchedulerHandleError> {
        let tasklet = self.run_queue_mut(queue)?.runnable.pop_front();
        if let Some(tasklet) = tasklet {
            self.tasklet_mut(tasklet)?.scheduled = false;
        }
        Ok(tasklet)
    }

    pub fn runnable_tasklet_count(
        &self,
        queue: CoreRunQueueId,
    ) -> Result<usize, CoreSchedulerHandleError> {
        Ok(self.run_queue(queue)?.runnable.len())
    }

    pub fn clear_run_queue(
        &mut self,
        queue: CoreRunQueueId,
    ) -> Result<Vec<CoreTaskletId>, CoreSchedulerHandleError> {
        let tasklets = self
            .run_queue_mut(queue)?
            .runnable
            .drain(..)
            .collect::<Vec<_>>();
        for tasklet in &tasklets {
            self.tasklet_mut(*tasklet)?.scheduled = false;
        }
        Ok(tasklets)
    }

    pub fn set_channel_preference(
        &mut self,
        channel: CoreChannelId,
        preference: i64,
    ) -> Result<(), CoreSchedulerHandleError> {
        self.channel_mut(channel)?.preference = preference.clamp(-1, 1);
        Ok(())
    }

    pub fn channel_snapshot(
        &self,
        channel: CoreChannelId,
    ) -> Result<CoreChannelSnapshot, CoreSchedulerHandleError> {
        let channel_state = self.channel(channel)?;
        Ok(channel_state.snapshot())
    }

    pub fn queue_front(
        &self,
        channel: CoreChannelId,
    ) -> Result<Option<CoreTaskletId>, CoreSchedulerHandleError> {
        let channel = self.channel(channel)?;
        Ok(channel
            .blocked_receivers
            .front()
            .copied()
            .or_else(|| channel.blocked_senders.front().copied()))
    }

    pub fn send(
        &mut self,
        sender: CoreTaskletId,
        channel: CoreChannelId,
    ) -> Result<CoreChannelOperationResult, CoreSchedulerHandleError> {
        self.ensure_tasklet(sender)?;
        self.ensure_channel(channel)?;

        if let Some(receiver) = self.channel_mut(channel)?.blocked_receivers.pop_front() {
            self.mark_runnable(sender)?;
            self.mark_runnable(receiver)?;
            self.update_channel_close_state(channel)?;
            let balance = self.channel(channel)?.balance();
            let preference = self.channel(channel)?.preference;
            return Ok(CoreChannelOperationResult::Matched {
                sender,
                receiver,
                channel,
                preferred: if preference >= 1 { sender } else { receiver },
                peer_runs_immediately: preference <= -1,
                balance,
            });
        }

        {
            let channel_state = self.channel(channel)?;
            if channel_state.closed || channel_state.closing {
                return Err(CoreSchedulerHandleError::ClosedChannel(channel));
            }
        }
        if self.tasklet(sender)?.block_trap {
            return Err(CoreSchedulerHandleError::BlockingDisallowed {
                tasklet: sender,
                channel,
                direction: CoreChannelDirection::Send,
            });
        }

        self.channel_mut(channel)?.blocked_senders.push_back(sender);
        self.mark_blocked(sender, channel, CoreChannelDirection::Send)?;
        let balance = self.channel(channel)?.balance();
        Ok(CoreChannelOperationResult::Blocked {
            tasklet: sender,
            channel,
            direction: CoreChannelDirection::Send,
            balance,
        })
    }

    pub fn receive(
        &mut self,
        receiver: CoreTaskletId,
        channel: CoreChannelId,
    ) -> Result<CoreChannelOperationResult, CoreSchedulerHandleError> {
        self.ensure_tasklet(receiver)?;
        self.ensure_channel(channel)?;

        if let Some(sender) = self.channel_mut(channel)?.blocked_senders.pop_front() {
            self.mark_runnable(sender)?;
            self.mark_runnable(receiver)?;
            self.update_channel_close_state(channel)?;
            let balance = self.channel(channel)?.balance();
            let preference = self.channel(channel)?.preference;
            return Ok(CoreChannelOperationResult::Matched {
                sender,
                receiver,
                channel,
                preferred: if preference >= 1 { sender } else { receiver },
                peer_runs_immediately: preference >= 1,
                balance,
            });
        }

        {
            let channel_state = self.channel(channel)?;
            if channel_state.closed || channel_state.closing {
                return Err(CoreSchedulerHandleError::ClosedChannel(channel));
            }
        }
        if self.tasklet(receiver)?.block_trap {
            return Err(CoreSchedulerHandleError::BlockingDisallowed {
                tasklet: receiver,
                channel,
                direction: CoreChannelDirection::Receive,
            });
        }

        self.channel_mut(channel)?
            .blocked_receivers
            .push_back(receiver);
        self.mark_blocked(receiver, channel, CoreChannelDirection::Receive)?;
        let balance = self.channel(channel)?.balance();
        Ok(CoreChannelOperationResult::Blocked {
            tasklet: receiver,
            channel,
            direction: CoreChannelDirection::Receive,
            balance,
        })
    }

    pub fn close_channel(
        &mut self,
        channel: CoreChannelId,
    ) -> Result<(), CoreSchedulerHandleError> {
        {
            let channel_state = self.channel_mut(channel)?;
            channel_state.closing = true;
        }
        self.update_channel_close_state(channel)
    }

    pub fn open_channel(&mut self, channel: CoreChannelId) -> Result<(), CoreSchedulerHandleError> {
        let channel_state = self.channel_mut(channel)?;
        channel_state.closing = false;
        channel_state.closed = false;
        Ok(())
    }

    pub fn clear_channel(
        &mut self,
        channel: CoreChannelId,
    ) -> Result<Vec<CoreTaskletId>, CoreSchedulerHandleError> {
        let removed = {
            let channel_state = self.channel_mut(channel)?;
            let mut removed = channel_state
                .blocked_receivers
                .drain(..)
                .collect::<Vec<_>>();
            removed.extend(channel_state.blocked_senders.drain(..));
            removed
        };
        for tasklet in &removed {
            let tasklet_state = self.tasklet_mut(*tasklet)?;
            tasklet_state.lifecycle = CoreTaskletLifecycle::Complete;
            tasklet_state.blocked_on = None;
            tasklet_state.alive = false;
            tasklet_state.scheduled = false;
            tasklet_state.paused = false;
        }
        self.update_channel_close_state(channel)?;
        Ok(removed)
    }

    pub fn remove_tasklet_from_channel(
        &mut self,
        tasklet: CoreTaskletId,
    ) -> Result<(), CoreSchedulerHandleError> {
        let Some(blocked_on) = self.tasklet(tasklet)?.blocked_on else {
            return Ok(());
        };
        match blocked_on.direction {
            CoreChannelDirection::Send => self
                .channel_mut(blocked_on.channel)?
                .blocked_senders
                .retain(|candidate| *candidate != tasklet),
            CoreChannelDirection::Receive => self
                .channel_mut(blocked_on.channel)?
                .blocked_receivers
                .retain(|candidate| *candidate != tasklet),
        }
        self.mark_runnable(tasklet)?;
        self.update_channel_close_state(blocked_on.channel)
    }

    fn ensure_tasklet(&self, tasklet: CoreTaskletId) -> Result<(), CoreSchedulerHandleError> {
        self.tasklet(tasklet).map(|_| ())
    }

    fn ensure_channel(&self, channel: CoreChannelId) -> Result<(), CoreSchedulerHandleError> {
        self.channel(channel).map(|_| ())
    }

    fn ensure_run_queue(&self, queue: CoreRunQueueId) -> Result<(), CoreSchedulerHandleError> {
        self.run_queue(queue).map(|_| ())
    }

    fn mark_blocked(
        &mut self,
        tasklet: CoreTaskletId,
        channel: CoreChannelId,
        direction: CoreChannelDirection,
    ) -> Result<(), CoreSchedulerHandleError> {
        let tasklet_state = self.tasklet_mut(tasklet)?;
        tasklet_state.lifecycle = CoreTaskletLifecycle::Blocked;
        tasklet_state.blocked_on = Some(CoreBlockedOnChannel { channel, direction });
        tasklet_state.alive = true;
        tasklet_state.scheduled = false;
        tasklet_state.paused = false;
        Ok(())
    }

    fn mark_runnable(&mut self, tasklet: CoreTaskletId) -> Result<(), CoreSchedulerHandleError> {
        let tasklet_state = self.tasklet_mut(tasklet)?;
        tasklet_state.lifecycle = CoreTaskletLifecycle::Runnable;
        tasklet_state.blocked_on = None;
        tasklet_state.alive = true;
        Ok(())
    }

    fn update_channel_close_state(
        &mut self,
        channel: CoreChannelId,
    ) -> Result<(), CoreSchedulerHandleError> {
        let should_close = {
            let channel_state = self.channel(channel)?;
            channel_state.closing && channel_state.balance() == 0
        };
        if should_close {
            self.channel_mut(channel)?.closed = true;
        }
        Ok(())
    }

    fn remove_tasklet_from_run_queues(&mut self, tasklet: CoreTaskletId) {
        for queue in self.run_queues.values_mut() {
            queue.runnable.retain(|candidate| *candidate != tasklet);
        }
    }

    fn tasklet(
        &self,
        tasklet: CoreTaskletId,
    ) -> Result<&CoreTaskletState, CoreSchedulerHandleError> {
        self.tasklets
            .get(&tasklet)
            .ok_or(CoreSchedulerHandleError::MissingTasklet(tasklet))
    }

    fn tasklet_mut(
        &mut self,
        tasklet: CoreTaskletId,
    ) -> Result<&mut CoreTaskletState, CoreSchedulerHandleError> {
        self.tasklets
            .get_mut(&tasklet)
            .ok_or(CoreSchedulerHandleError::MissingTasklet(tasklet))
    }

    fn channel(
        &self,
        channel: CoreChannelId,
    ) -> Result<&CoreChannelState, CoreSchedulerHandleError> {
        self.channels
            .get(&channel)
            .ok_or(CoreSchedulerHandleError::MissingChannel(channel))
    }

    fn channel_mut(
        &mut self,
        channel: CoreChannelId,
    ) -> Result<&mut CoreChannelState, CoreSchedulerHandleError> {
        self.channels
            .get_mut(&channel)
            .ok_or(CoreSchedulerHandleError::MissingChannel(channel))
    }

    fn run_queue(
        &self,
        queue: CoreRunQueueId,
    ) -> Result<&CoreRunQueueState, CoreSchedulerHandleError> {
        self.run_queues
            .get(&queue)
            .ok_or(CoreSchedulerHandleError::MissingRunQueue(queue))
    }

    fn run_queue_mut(
        &mut self,
        queue: CoreRunQueueId,
    ) -> Result<&mut CoreRunQueueState, CoreSchedulerHandleError> {
        self.run_queues
            .get_mut(&queue)
            .ok_or(CoreSchedulerHandleError::MissingRunQueue(queue))
    }
}

impl CoreChannelState {
    fn balance(&self) -> i64 {
        self.blocked_senders.len() as i64 - self.blocked_receivers.len() as i64
    }

    fn snapshot(&self) -> CoreChannelSnapshot {
        CoreChannelSnapshot {
            preference: self.preference,
            closing: self.closing,
            closed: self.closed,
            balance: self.balance(),
            blocked_senders: self.blocked_senders.iter().copied().collect(),
            blocked_receivers: self.blocked_receivers.iter().copied().collect(),
        }
    }
}

impl CoreTaskletState {
    fn snapshot(&self) -> CoreTaskletSnapshot {
        CoreTaskletSnapshot {
            lifecycle: self.lifecycle,
            blocked_on: self.blocked_on,
            block_trap: self.block_trap,
            alive: self.alive,
            scheduled: self.scheduled,
            paused: self.paused,
            times_switched_to: self.times_switched_to,
        }
    }
}

pub fn run_scenario(scenario: &Scenario) -> Result<TraceRun, SchedulerError> {
    let mut runtime = Runtime::new(scenario)?;
    runtime.run()?;
    let final_state = runtime.final_state();
    Ok(TraceRun {
        schema: SEMANTIC_TRACE_SCHEMA,
        events: runtime.events,
        final_state,
    })
}

fn default_channel_preference() -> i64 {
    -1
}

fn default_nested_tasklets() -> bool {
    true
}

fn default_initially_scheduled() -> bool {
    true
}

fn default_initially_bound() -> bool {
    true
}

#[derive(Debug, Clone)]
struct TaskletState {
    body: Vec<Operation>,
    pc: usize,
    depth: usize,
    callable_bound: bool,
    alive: bool,
    scheduled: bool,
    blocked: bool,
    paused: bool,
    times_switched_to: u64,
    blocked_on: Option<String>,
    blocked_direction: Option<BlockDirection>,
    resume_after_completion: Option<String>,
    vars: BTreeMap<String, Value>,
    raised: Option<Value>,
    pending_exit: bool,
    block_trap: bool,
}

#[derive(Debug, Clone)]
struct ChannelState {
    preference: i64,
    closing: bool,
    closed: bool,
    blocked_senders: VecDeque<BlockedSender>,
    blocked_receivers: VecDeque<BlockedReceiver>,
}

#[derive(Debug, Clone)]
struct BlockedSender {
    tasklet: String,
    payload: TransferPayload,
}

#[derive(Debug, Clone)]
struct BlockedReceiver {
    tasklet: String,
    bind: Option<String>,
}

#[derive(Debug, Clone)]
enum TransferPayload {
    Value(Value),
    Exception {
        exception: String,
        args: Value,
    },
    Throw {
        exception: String,
        value: Value,
        traceback: Option<String>,
    },
}

#[derive(Debug, Clone, Copy)]
enum BlockDirection {
    Send,
    Receive,
}

impl BlockDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Send => "send",
            Self::Receive => "receive",
        }
    }
}

impl TransferPayload {
    fn trace_value(&self) -> Value {
        match self {
            Self::Value(value) => value.clone(),
            Self::Exception { exception, args } => json!({
                "type": "exception",
                "exception": exception,
                "args": args
            }),
            Self::Throw {
                exception,
                value,
                traceback,
            } => json!({
                "type": "throw",
                "exception": exception,
                "value": value,
                "traceback": traceback
            }),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Value(_) => "value",
            Self::Exception { .. } => "exception",
            Self::Throw { .. } => "throw",
        }
    }

    fn as_error_json(&self) -> Value {
        match self {
            Self::Value(value) => json!({
                "type": "Value",
                "value": value
            }),
            Self::Exception { exception, args } => json!({
                "type": exception,
                "args": args
            }),
            Self::Throw {
                exception,
                value,
                traceback,
            } => json!({
                "type": exception,
                "value": value,
                "traceback": traceback
            }),
        }
    }
}

#[derive(Debug)]
struct Runtime {
    scenario: Scenario,
    tasklets: BTreeMap<String, TaskletState>,
    channels: BTreeMap<String, ChannelState>,
    runnable: VecDeque<String>,
    current: String,
    seq: u64,
    events: Vec<Value>,
    observations: BTreeMap<String, Vec<Value>>,
    error: Option<Value>,
    main_block_trap: bool,
    switch_trap_level: i64,
    last_timeout_completed_tasklets: usize,
    last_timeout_switched_tasklets: usize,
}

#[derive(Debug)]
enum TaskletOutcome {
    Blocked,
    Paused,
    Complete(String),
}

#[derive(Debug)]
enum StepOutcome {
    Continue,
    Blocked,
    Paused,
    Transfer {
        to: String,
        return_to: Option<String>,
    },
}

#[derive(Debug, Default)]
struct SchedulerRunStats {
    completed: usize,
    switches: usize,
}

impl Runtime {
    fn new(scenario: &Scenario) -> Result<Self, SchedulerError> {
        let mut tasklets = BTreeMap::new();
        for tasklet in &scenario.tasklets {
            if tasklets
                .insert(
                    tasklet.id.clone(),
                    TaskletState {
                        body: tasklet.body.clone(),
                        pc: 0,
                        depth: 0,
                        callable_bound: tasklet.initially_bound,
                        alive: tasklet.initially_bound,
                        scheduled: false,
                        blocked: false,
                        paused: false,
                        times_switched_to: 0,
                        blocked_on: None,
                        blocked_direction: None,
                        resume_after_completion: None,
                        vars: BTreeMap::new(),
                        raised: None,
                        pending_exit: false,
                        block_trap: false,
                    },
                )
                .is_some()
            {
                return Err(SchedulerError::DuplicateTasklet(tasklet.id.clone()));
            }
        }

        let mut channels = BTreeMap::new();
        for channel in &scenario.channels {
            if channels
                .insert(
                    channel.id.clone(),
                    ChannelState {
                        preference: channel.preference.clamp(-1, 1),
                        closing: false,
                        closed: false,
                        blocked_senders: VecDeque::new(),
                        blocked_receivers: VecDeque::new(),
                    },
                )
                .is_some()
            {
                return Err(SchedulerError::DuplicateChannel(channel.id.clone()));
            }
        }

        Ok(Self {
            scenario: scenario.clone(),
            tasklets,
            channels,
            runnable: VecDeque::from([String::from("main")]),
            current: String::from("main"),
            seq: 0,
            events: Vec::new(),
            observations: BTreeMap::new(),
            error: None,
            main_block_trap: false,
            switch_trap_level: 0,
            last_timeout_completed_tasklets: 0,
            last_timeout_switched_tasklets: 0,
        })
    }

    fn run(&mut self) -> Result<(), SchedulerError> {
        self.emit(
            "scheduler.start",
            [
                ("actor", json!("main")),
                ("current", json!(self.current)),
                ("run_count", json!(self.run_count())),
                ("runnable", json!(self.runnable_snapshot())),
            ],
        );

        let channels = self.scenario.channels.clone();
        for channel in &channels {
            self.emit(
                "channel.new",
                [
                    ("actor", json!("main")),
                    ("channel", json!(channel.id)),
                    ("preference", json!(channel.preference.clamp(-1, 1))),
                    ("balance", json!(0)),
                ],
            );
        }

        if self.uses_batch_new_for_main_deadlock() {
            let ids = self.scenario_tasklet_ids();
            for id in &ids {
                self.schedule_new_tasklet(id)?;
            }
            self.emit(
                "tasklet.batch_new",
                [
                    ("actor", json!("main")),
                    ("tasklets", json!(ids)),
                    ("run_count", json!(self.run_count())),
                ],
            );
        } else {
            let tasklets = self.scenario.tasklets.clone();
            for tasklet in &tasklets {
                if !tasklet.initially_scheduled || !tasklet.initially_bound {
                    continue;
                }
                self.schedule_new_tasklet(&tasklet.id)?;
                self.emit(
                    "tasklet.new",
                    [
                        ("actor", json!("main")),
                        ("tasklet", json!(tasklet.id)),
                        ("alive", json!(true)),
                        ("scheduled", json!(true)),
                        ("run_count", json!(self.run_count())),
                        ("runnable", json!(self.runnable_snapshot())),
                    ],
                );
            }
        }

        match self.scenario.entrypoint.clone() {
            Entrypoint::RunScheduler => self.run_scheduler(),
            Entrypoint::RunSchedulerN { count } => self.run_scheduler_n(count),
            Entrypoint::RunSchedulerWithTimeout { timeout_ns } => {
                self.run_scheduler_with_timeout(timeout_ns)
            }
            Entrypoint::RunTasklet { tasklet } => self.run_tasklet_entry(&tasklet),
            Entrypoint::RunSchedulerThenSend { channel, value } => {
                self.run_scheduler()?;
                self.main_send_entry(&channel, value)
            }
            Entrypoint::RunSchedulerThenSendThenRun { channel, value } => {
                self.run_scheduler()?;
                self.main_send_entry(&channel, value)?;
                self.run_scheduler()
            }
            Entrypoint::Receive { actor, channel } if actor == "main" => {
                self.main_receive_entry(&channel)
            }
            Entrypoint::Receive { actor, .. } => Err(SchedulerError::UnsupportedMainOperation(
                format!("receive by {actor}"),
            )),
            Entrypoint::Script { steps } => self.run_script(&steps),
        }
    }

    fn run_script(&mut self, steps: &[MainStep]) -> Result<(), SchedulerError> {
        for step in steps {
            match step {
                MainStep::Schedule => {
                    self.schedule_main(false)?;
                }
                MainStep::ScheduleRemove => {
                    self.schedule_main(true)?;
                }
                MainStep::RunScheduler => self.run_scheduler()?,
                MainStep::RunSchedulerN { count } => self.run_scheduler_n(*count)?,
                MainStep::RunSchedulerWithTimeout { timeout_ns } => {
                    self.run_scheduler_with_timeout(*timeout_ns)?
                }
                MainStep::Send { channel, value } => {
                    if self.reject_if_switch_trapped("main", "send") {
                        continue;
                    }
                    self.main_send_entry(channel, value.clone())?;
                }
                MainStep::SendException {
                    channel,
                    exception,
                    args,
                } => {
                    if self.reject_if_switch_trapped("main", "send_exception") {
                        continue;
                    }
                    self.main_send_payload(
                        channel,
                        TransferPayload::Exception {
                            exception: exception.clone(),
                            args: args.clone(),
                        },
                    )?;
                }
                MainStep::SendThrow {
                    channel,
                    exception,
                    value,
                    traceback,
                } => {
                    if self.reject_if_switch_trapped("main", "send_throw") {
                        continue;
                    }
                    self.main_send_payload(
                        channel,
                        TransferPayload::Throw {
                            exception: exception.clone(),
                            value: value.clone(),
                            traceback: traceback.clone(),
                        },
                    )?;
                }
                MainStep::SetBlockTrap { value } => {
                    self.set_main_block_trap(*value);
                }
                MainStep::SwitchTrap { delta } => {
                    self.set_switch_trap(*delta);
                }
                MainStep::Receive { channel, bind } => {
                    if self.reject_if_switch_trapped("main", "receive") {
                        continue;
                    }
                    self.main_receive_payload(channel, bind.as_deref())?;
                }
                MainStep::Close { channel } => self.close_channel("main", channel)?,
                MainStep::Open { channel } => self.open_channel("main", channel)?,
                MainStep::Clear { channel, pending } => {
                    self.clear_channel("main", channel, *pending)?
                }
                MainStep::Remove { tasklet } => {
                    self.remove_tasklet("main", tasklet)?;
                }
                MainStep::Insert { tasklet } => {
                    self.insert_tasklet("main", tasklet)?;
                }
                MainStep::Kill { tasklet, pending } => {
                    self.tasklet(tasklet)?;
                    if self.reject_if_switch_trapped("main", "kill") {
                        continue;
                    }
                    self.kill_tasklet("main", tasklet, *pending)?;
                }
                MainStep::Switch { tasklet } => {
                    self.switch_tasklet_entry(tasklet)?;
                }
                MainStep::RunTasklet { tasklet } => {
                    self.run_tasklet_entry(tasklet)?;
                }
                MainStep::Bind {
                    tasklet,
                    body,
                    args_bound,
                } => {
                    self.bind_tasklet("main", tasklet, body.clone(), *args_bound)?;
                }
                MainStep::Setup { tasklet } => {
                    self.setup_tasklet("main", tasklet)?;
                }
                MainStep::Unbind { tasklet } => {
                    self.unbind_tasklet("main", tasklet)?;
                }
                MainStep::RaiseException { tasklet, exception } => {
                    self.tasklet(tasklet)?;
                    if self.reject_if_switch_trapped("main", "raise_exception") {
                        continue;
                    }
                    self.raise_exception_on_tasklet(tasklet, exception, "main")?;
                }
                MainStep::QueueFront { channel, target } => {
                    self.probe_channel_queue("main", channel, target.as_deref())?;
                }
            }
        }
        Ok(())
    }

    fn schedule_main(&mut self, remove: bool) -> Result<(), SchedulerError> {
        let operation = if remove {
            "schedule_remove"
        } else {
            "schedule"
        };
        if self.reject_if_switch_trapped("main", operation) {
            return Ok(());
        }
        let kind = if remove {
            "scheduler.schedule_remove"
        } else {
            "scheduler.schedule"
        };
        self.emit(
            kind,
            [
                ("actor", json!("main")),
                ("tasklet", json!("main")),
                ("run_count", json!(self.run_count())),
                ("runnable", json!(self.runnable_snapshot())),
            ],
        );
        Ok(())
    }

    fn run_scheduler(&mut self) -> Result<(), SchedulerError> {
        if self.reject_if_switch_trapped("main", "run") {
            return Ok(());
        }
        self.run_scheduler_with_limit(None, json!("until_idle"))
            .map(|_| ())
    }

    fn run_scheduler_n(&mut self, count: usize) -> Result<(), SchedulerError> {
        if self.reject_if_switch_trapped("main", "run") {
            return Ok(());
        }
        self.run_scheduler_with_limit(Some(count), json!(count))
            .map(|_| ())
    }

    fn run_scheduler_with_timeout(&mut self, timeout_ns: i64) -> Result<(), SchedulerError> {
        if self.reject_if_switch_trapped("main", "run_for_time") {
            return Ok(());
        }
        self.last_timeout_completed_tasklets = 0;
        self.last_timeout_switched_tasklets = 0;
        let tasklet_limit = if timeout_ns <= 0 { Some(1) } else { None };
        let stats =
            self.run_scheduler_with_limit(tasklet_limit, json!({ "timeout_ns": timeout_ns }))?;
        self.last_timeout_completed_tasklets = stats.completed;
        self.last_timeout_switched_tasklets = stats.switches;
        Ok(())
    }

    fn run_scheduler_with_limit(
        &mut self,
        limit: Option<usize>,
        limit_value: Value,
    ) -> Result<SchedulerRunStats, SchedulerError> {
        self.emit(
            "scheduler.run.start",
            [("actor", json!("main")), ("limit", limit_value)],
        );

        let mut stats = SchedulerRunStats::default();
        let mut ran = 0;
        while limit.map_or(true, |limit| ran < limit) {
            let Some(next) = self.pop_next_runnable_tasklet() else {
                break;
            };
            ran += 1;
            let from = self.current.clone();
            self.switch(&from, &next, "next_runnable");
            stats.switches += 1;
            self.current = next.clone();

            match self.run_tasklet_chain(&next, None)? {
                TaskletOutcome::Blocked | TaskletOutcome::Paused => {}
                TaskletOutcome::Complete(tasklet) => {
                    stats.completed += 1;
                    let reason = if self.has_runnable_tasklets() {
                        "tasklet_complete"
                    } else {
                        "run_complete"
                    };
                    self.switch(&tasklet, "main", reason);
                    stats.switches += 1;
                    self.current = String::from("main");
                }
            }
        }

        Ok(stats)
    }

    fn run_tasklet_entry(&mut self, tasklet: &str) -> Result<(), SchedulerError> {
        self.ensure_tasklet_can_run(tasklet, "run")?;
        if self.reject_if_switch_trapped("main", "tasklet.run") {
            return Ok(());
        }

        let nested_tail = self.direct_run_nested_tail(tasklet)?;

        self.remove_from_runnable(tasklet);
        self.set_scheduled(tasklet, false)?;

        let from = self.current.clone();
        self.switch(&from, tasklet, "run_tasklet");
        self.current = tasklet.to_string();

        match self.run_tasklet_chain(tasklet, None)? {
            TaskletOutcome::Blocked | TaskletOutcome::Paused => Ok(()),
            TaskletOutcome::Complete(done) => {
                let reason =
                    if self.scenario.nested_tasklets && self.has_runnable_from(&nested_tail) {
                        "tasklet_complete"
                    } else if !self.scenario.nested_tasklets && self.has_runnable_tasklets() {
                        "tasklet_complete"
                    } else {
                        "run_complete"
                    };
                self.switch(&done, "main", reason);
                self.current = String::from("main");
                if self.scenario.nested_tasklets {
                    self.run_nested_tail(&nested_tail)
                } else {
                    self.run_remaining_after_direct_tasklet_run()
                }
            }
        }
    }

    fn run_tasklet_operation(
        &mut self,
        caller: &str,
        target: &str,
    ) -> Result<StepOutcome, SchedulerError> {
        self.ensure_tasklet_can_run(target, "run")?;

        self.advance_pc(caller)?;
        let nested_tail = self.direct_run_nested_tail(target)?;
        self.remove_from_runnable(target);
        self.set_scheduled(target, false)?;

        self.switch(caller, target, "run_tasklet");
        self.current = target.to_string();

        match self.run_tasklet_chain(target, None)? {
            TaskletOutcome::Blocked => Ok(StepOutcome::Blocked),
            TaskletOutcome::Paused => Ok(StepOutcome::Continue),
            TaskletOutcome::Complete(done) => {
                let reason =
                    if self.scenario.nested_tasklets && self.has_runnable_from(&nested_tail) {
                        "tasklet_complete"
                    } else if !self.scenario.nested_tasklets && self.has_runnable_tasklets() {
                        "tasklet_complete"
                    } else {
                        "run_complete"
                    };
                self.switch(&done, "main", reason);
                self.current = String::from("main");

                if self.scenario.nested_tasklets {
                    self.run_nested_tail(&nested_tail)?;
                } else {
                    self.run_remaining_after_direct_tasklet_run()?;
                }

                let from = self.current.clone();
                self.switch(&from, caller, "resume_tasklet_runner");
                self.current = caller.to_string();
                Ok(StepOutcome::Continue)
            }
        }
    }

    fn run_nested_tail(&mut self, nested_tail: &[String]) -> Result<(), SchedulerError> {
        for (index, tasklet) in nested_tail.iter().enumerate() {
            if !self.is_runnable_tasklet(tasklet) {
                continue;
            }
            self.remove_from_runnable(tasklet);
            self.set_scheduled(tasklet, false)?;
            let from = self.current.clone();
            self.switch(&from, tasklet, "nested_next_runnable");
            self.current = tasklet.clone();
            match self.run_tasklet_chain(tasklet, None)? {
                TaskletOutcome::Blocked | TaskletOutcome::Paused => return Ok(()),
                TaskletOutcome::Complete(done) => {
                    let has_more_tail = nested_tail[index + 1..]
                        .iter()
                        .any(|id| self.is_runnable_tasklet(id));
                    let reason = if has_more_tail {
                        "tasklet_complete"
                    } else {
                        "run_complete"
                    };
                    self.switch(&done, "main", reason);
                    self.current = String::from("main");
                }
            }
        }
        Ok(())
    }

    fn run_remaining_after_direct_tasklet_run(&mut self) -> Result<(), SchedulerError> {
        while let Some(next) = self.pop_next_runnable_tasklet() {
            let from = self.current.clone();
            self.switch(&from, &next, "next_runnable");
            self.current = next.clone();

            match self.run_tasklet_chain(&next, None)? {
                TaskletOutcome::Blocked | TaskletOutcome::Paused => {}
                TaskletOutcome::Complete(tasklet) => {
                    let reason = if self.has_runnable_tasklets() {
                        "tasklet_complete"
                    } else {
                        "run_complete"
                    };
                    self.switch(&tasklet, "main", reason);
                    self.current = String::from("main");
                }
            }
        }

        Ok(())
    }

    fn run_tasklet_chain(
        &mut self,
        tasklet: &str,
        return_to: Option<String>,
    ) -> Result<TaskletOutcome, SchedulerError> {
        let current = tasklet.to_string();
        loop {
            if self.tasklet(&current)?.pending_exit {
                self.tasklet_mut(&current)?.pending_exit = false;
                self.emit(
                    "tasklet.exit",
                    [
                        ("actor", json!(current)),
                        ("tasklet", json!(current)),
                        ("exception", json!("TaskletExit")),
                        ("pending", json!(true)),
                    ],
                );
                let resume_after_completion =
                    self.tasklet_mut(&current)?.resume_after_completion.take();
                self.complete_tasklet(&current)?;
                if let Some(resume) = resume_after_completion {
                    self.switch(&current, &resume, "resume_channel_sender");
                    self.current = resume.clone();
                    return self.run_tasklet_chain(&resume, None);
                }
                if let Some(return_to) = return_to {
                    self.switch(&current, &return_to, "resume_channel_sender");
                    self.current = return_to.clone();
                    return self.run_tasklet_chain(&return_to, None);
                }
                return Ok(TaskletOutcome::Complete(current));
            }

            if let Some(raised) = self.tasklet_mut(&current)?.raised.take() {
                self.emit(
                    "tasklet.exception",
                    [
                        ("actor", json!(current)),
                        ("tasklet", json!(current)),
                        ("exception", raised.clone()),
                    ],
                );
                self.complete_tasklet(&current)?;
                self.tasklet_mut(&current)?.raised = Some(raised);
                return Ok(TaskletOutcome::Complete(current));
            }

            let op = {
                let state = self
                    .tasklets
                    .get(&current)
                    .ok_or_else(|| SchedulerError::MissingTasklet(current.clone()))?;
                state.body.get(state.pc).cloned()
            };

            let Some(op) = op else {
                let resume_after_completion =
                    self.tasklet_mut(&current)?.resume_after_completion.take();
                self.complete_tasklet(&current)?;
                if let Some(resume) = resume_after_completion {
                    self.switch(&current, &resume, "resume_channel_sender");
                    self.current = resume.clone();
                    return self.run_tasklet_chain(&resume, None);
                }
                if let Some(return_to) = return_to {
                    self.switch(&current, &return_to, "resume_channel_sender");
                    self.current = return_to.clone();
                    return self.run_tasklet_chain(&return_to, None);
                }
                return Ok(TaskletOutcome::Complete(current));
            };

            match self.step_tasklet_operation(&current, op)? {
                StepOutcome::Continue => {}
                StepOutcome::Blocked => return Ok(TaskletOutcome::Blocked),
                StepOutcome::Paused => return Ok(TaskletOutcome::Paused),
                StepOutcome::Transfer { to, return_to } => {
                    return self.run_tasklet_chain(&to, return_to);
                }
            }
        }
    }

    fn step_tasklet_operation(
        &mut self,
        tasklet: &str,
        op: Operation,
    ) -> Result<StepOutcome, SchedulerError> {
        match op {
            Operation::Append { target, value } => {
                self.observations
                    .entry(target.clone())
                    .or_default()
                    .push(value.clone());
                self.advance_pc(tasklet)?;

                let mut fields = vec![
                    (String::from("actor"), json!(tasklet)),
                    (String::from("target"), json!(target.clone())),
                    (String::from("value"), value),
                ];
                if let Some(values) = self.observations.get(&target) {
                    fields.push((target, json!(values)));
                }
                self.emit("tasklet.observed", fields);

                Ok(StepOutcome::Continue)
            }
            Operation::Spawn { tasklet: child } => {
                self.spawn_tasklet(tasklet, child)?;
                self.advance_pc(tasklet)?;
                Ok(StepOutcome::Continue)
            }
            Operation::RunTasklet { tasklet: target } => {
                self.run_tasklet_operation(tasklet, &target)
            }
            Operation::Schedule => {
                self.advance_pc(tasklet)?;
                self.emit(
                    "scheduler.schedule",
                    [
                        ("actor", json!(tasklet)),
                        ("run_count", json!(self.run_count())),
                    ],
                );
                Ok(StepOutcome::Continue)
            }
            Operation::ScheduleRemove => {
                self.advance_pc(tasklet)?;
                {
                    let state = self.tasklet_mut(tasklet)?;
                    state.scheduled = false;
                    state.paused = true;
                }
                self.emit(
                    "scheduler.schedule_remove",
                    [
                        ("actor", json!(tasklet)),
                        ("tasklet", json!(tasklet)),
                        ("alive", json!(true)),
                        ("scheduled", json!(false)),
                        ("paused", json!(true)),
                        ("run_count", json!(self.run_count())),
                        ("runnable", json!(self.runnable_snapshot())),
                    ],
                );
                self.switch(tasklet, "main", "schedule_remove");
                self.current = String::from("main");
                Ok(StepOutcome::Paused)
            }
            Operation::SwitchTrap { delta } => {
                self.set_switch_trap(delta);
                self.advance_pc(tasklet)?;
                Ok(StepOutcome::Continue)
            }
            Operation::Send { channel, value } => self.send_from_tasklet(tasklet, &channel, value),
            Operation::SendException {
                channel,
                exception,
                args,
            } => self.send_payload_from_tasklet(
                tasklet,
                &channel,
                TransferPayload::Exception { exception, args },
            ),
            Operation::SendThrow {
                channel,
                exception,
                value,
                traceback,
            } => self.send_payload_from_tasklet(
                tasklet,
                &channel,
                TransferPayload::Throw {
                    exception,
                    value,
                    traceback,
                },
            ),
            Operation::SetBlockTrap { value } => {
                self.set_tasklet_block_trap(tasklet, value)?;
                self.advance_pc(tasklet)?;
                Ok(StepOutcome::Continue)
            }
            Operation::Receive { channel, bind } => {
                self.receive_from_tasklet(tasklet, &channel, bind)
            }
            Operation::Close { channel } => {
                self.advance_pc(tasklet)?;
                self.close_channel(tasklet, &channel)?;
                Ok(StepOutcome::Continue)
            }
            Operation::Open { channel } => {
                self.advance_pc(tasklet)?;
                self.open_channel(tasklet, &channel)?;
                Ok(StepOutcome::Continue)
            }
            Operation::Clear { channel, pending } => {
                self.advance_pc(tasklet)?;
                self.clear_channel(tasklet, &channel, pending)?;
                Ok(StepOutcome::Continue)
            }
            Operation::QueueFront { channel, target } => {
                self.advance_pc(tasklet)?;
                self.probe_channel_queue(tasklet, &channel, target.as_deref())?;
                Ok(StepOutcome::Continue)
            }
            Operation::RemoveTasklet { tasklet: target } => {
                self.advance_pc(tasklet)?;
                self.remove_tasklet(tasklet, &target)?;
                Ok(StepOutcome::Continue)
            }
            Operation::InsertTasklet { tasklet: target } => {
                self.advance_pc(tasklet)?;
                self.insert_tasklet(tasklet, &target)?;
                Ok(StepOutcome::Continue)
            }
            Operation::KillTasklet {
                tasklet: target,
                pending,
            } => {
                self.advance_pc(tasklet)?;
                self.kill_tasklet(tasklet, &target, pending)?;
                Ok(StepOutcome::Continue)
            }
            Operation::SwitchTasklet { tasklet: target } => {
                self.switch_tasklet_operation(tasklet, &target)
            }
            Operation::BindTasklet {
                tasklet: target,
                body,
                args_bound,
            } => {
                self.advance_pc(tasklet)?;
                self.bind_tasklet(tasklet, &target, body, args_bound)?;
                Ok(StepOutcome::Continue)
            }
            Operation::SetupTasklet { tasklet: target } => {
                self.advance_pc(tasklet)?;
                self.setup_tasklet(tasklet, &target)?;
                Ok(StepOutcome::Continue)
            }
            Operation::UnbindTasklet { tasklet: target } => {
                self.advance_pc(tasklet)?;
                self.unbind_tasklet(tasklet, &target)?;
                Ok(StepOutcome::Continue)
            }
        }
    }

    fn send_from_tasklet(
        &mut self,
        sender: &str,
        channel_id: &str,
        value: Value,
    ) -> Result<StepOutcome, SchedulerError> {
        self.send_payload_from_tasklet(sender, channel_id, TransferPayload::Value(value))
    }

    fn send_payload_from_tasklet(
        &mut self,
        sender: &str,
        channel_id: &str,
        payload: TransferPayload,
    ) -> Result<StepOutcome, SchedulerError> {
        self.emit_channel_callback(
            channel_id,
            sender,
            true,
            self.channel(channel_id)?.blocked_receivers.is_empty(),
        );

        let waiting_receivers = self
            .channel(channel_id)?
            .blocked_receivers
            .iter()
            .map(|receiver| receiver.tasklet.clone())
            .collect::<Vec<_>>();
        self.emit_send_attempt(sender, channel_id, &payload, waiting_receivers);

        let blocked_receiver = self.channel_mut(channel_id)?.blocked_receivers.pop_front();

        if let Some(receiver) = blocked_receiver {
            self.unblock_tasklet(&receiver.tasklet)?;
            let receiver_raised =
                self.deliver_payload_to_tasklet(&receiver.tasklet, receiver.bind, &payload)?;
            self.advance_pc(&receiver.tasklet)?;
            self.advance_pc(sender)?;

            let preference = self.channel(channel_id)?.preference;
            let preferred = preferred_continuation(preference, sender, &receiver.tasklet);
            self.update_channel_close_state(channel_id)?;
            self.emit_transfer(channel_id, sender, &receiver.tasklet, &payload, preferred);

            if receiver_raised {
                self.tasklet_error_from_receive(&receiver.tasklet)?;
                return Ok(StepOutcome::Continue);
            }

            if preference <= -1 {
                self.switch(sender, &receiver.tasklet, "channel_preference_receiver");
                self.current = receiver.tasklet.clone();
                Ok(StepOutcome::Transfer {
                    to: receiver.tasklet,
                    return_to: Some(sender.to_string()),
                })
            } else {
                self.schedule_new_tasklet(&receiver.tasklet)?;
                Ok(StepOutcome::Continue)
            }
        } else {
            if self.tasklet(sender)?.block_trap {
                self.tasklet_channel_error(
                    sender,
                    channel_id,
                    "RuntimeError",
                    "Channel cannot block on main tasklet with block_trap set true",
                )?;
                return Ok(StepOutcome::Continue);
            }

            if self.channel(channel_id)?.closed || self.channel(channel_id)?.closing {
                self.tasklet_channel_error(
                    sender,
                    channel_id,
                    "ValueError",
                    "Send operation on a closed channel",
                )?;
                return Ok(StepOutcome::Continue);
            }

            self.channel_mut(channel_id)?
                .blocked_senders
                .push_back(BlockedSender {
                    tasklet: sender.to_string(),
                    payload,
                });
            self.block_tasklet(sender, channel_id, BlockDirection::Send)?;

            let balance = self.channel(channel_id)?.balance();
            self.emit(
                "channel.block",
                [
                    ("actor", json!(sender)),
                    ("channel", json!(channel_id)),
                    ("direction", json!("send")),
                    ("balance", json!(balance)),
                    ("blocked_senders", json!(self.blocked_senders(channel_id)?)),
                    (
                        "blocked_receivers",
                        json!(self.blocked_receivers(channel_id)?),
                    ),
                ],
            );

            self.switch(sender, "main", "blocked");
            self.current = String::from("main");
            Ok(StepOutcome::Blocked)
        }
    }

    fn receive_from_tasklet(
        &mut self,
        receiver: &str,
        channel_id: &str,
        bind: Option<String>,
    ) -> Result<StepOutcome, SchedulerError> {
        self.emit_channel_callback(
            channel_id,
            receiver,
            false,
            self.channel(channel_id)?.blocked_senders.is_empty(),
        );

        let waiting_senders = self
            .channel(channel_id)?
            .blocked_senders
            .iter()
            .map(|sender| sender.tasklet.clone())
            .collect::<Vec<_>>();
        self.emit(
            "channel.receive.attempt",
            [
                ("actor", json!(receiver)),
                ("channel", json!(channel_id)),
                ("waiting_senders", json!(waiting_senders)),
            ],
        );

        let blocked_sender = self.channel_mut(channel_id)?.blocked_senders.pop_front();

        if let Some(sender) = blocked_sender {
            self.unblock_tasklet(&sender.tasklet)?;
            let receiver_raised =
                self.deliver_payload_to_tasklet(receiver, bind, &sender.payload)?;
            self.advance_pc(&sender.tasklet)?;
            self.advance_pc(receiver)?;
            if self.channel(channel_id)?.preference < 1 {
                self.tasklet_mut(receiver)?.resume_after_completion = Some(sender.tasklet.clone());
            }

            let preference = self.channel(channel_id)?.preference;
            let preferred = preferred_continuation(preference, &sender.tasklet, receiver);
            self.update_channel_close_state(channel_id)?;
            self.emit_transfer(
                channel_id,
                &sender.tasklet,
                receiver,
                &sender.payload,
                preferred,
            );

            if receiver_raised {
                self.tasklet_error_from_receive(receiver)?;
            }

            if preference >= 1 {
                self.schedule_new_tasklet(receiver)?;
                self.switch(receiver, &sender.tasklet, "channel_preference_sender");
                self.current = sender.tasklet.clone();
                Ok(StepOutcome::Transfer {
                    to: sender.tasklet,
                    return_to: None,
                })
            } else {
                Ok(StepOutcome::Continue)
            }
        } else {
            if self.tasklet(receiver)?.block_trap {
                self.tasklet_channel_error(
                    receiver,
                    channel_id,
                    "RuntimeError",
                    "Channel cannot block on main tasklet with block_trap set true",
                )?;
                return Ok(StepOutcome::Continue);
            }

            if self.channel(channel_id)?.closed || self.channel(channel_id)?.closing {
                self.tasklet_channel_error(
                    receiver,
                    channel_id,
                    "ValueError",
                    "receive operation on a closed channel",
                )?;
                return Ok(StepOutcome::Continue);
            }

            self.channel_mut(channel_id)?
                .blocked_receivers
                .push_back(BlockedReceiver {
                    tasklet: receiver.to_string(),
                    bind,
                });
            self.block_tasklet(receiver, channel_id, BlockDirection::Receive)?;

            let balance = self.channel(channel_id)?.balance();
            self.emit(
                "channel.block",
                [
                    ("actor", json!(receiver)),
                    ("channel", json!(channel_id)),
                    ("direction", json!("receive")),
                    ("balance", json!(balance)),
                    ("blocked_senders", json!(self.blocked_senders(channel_id)?)),
                    (
                        "blocked_receivers",
                        json!(self.blocked_receivers(channel_id)?),
                    ),
                ],
            );

            self.switch(receiver, "main", "blocked");
            self.current = String::from("main");
            Ok(StepOutcome::Blocked)
        }
    }

    fn main_receive_entry(&mut self, channel_id: &str) -> Result<(), SchedulerError> {
        self.main_receive_payload(channel_id, None)
    }

    fn main_receive_payload(
        &mut self,
        channel_id: &str,
        bind: Option<&str>,
    ) -> Result<(), SchedulerError> {
        self.emit_channel_callback(
            channel_id,
            "main",
            false,
            self.channel(channel_id)?.blocked_senders.is_empty(),
        );

        let waiting_senders = self
            .channel(channel_id)?
            .blocked_senders
            .iter()
            .map(|sender| sender.tasklet.clone())
            .collect::<Vec<_>>();
        self.emit(
            "channel.receive.attempt",
            [
                ("actor", json!("main")),
                ("channel", json!(channel_id)),
                ("waiting_senders", json!(waiting_senders)),
            ],
        );

        let blocked_sender = self.channel_mut(channel_id)?.blocked_senders.pop_front();
        if let Some(sender) = blocked_sender {
            self.unblock_tasklet(&sender.tasklet)?;
            self.advance_pc(&sender.tasklet)?;
            self.update_channel_close_state(channel_id)?;
            match &sender.payload {
                TransferPayload::Value(value) => {
                    if let Some(bind) = bind {
                        self.observations
                            .entry(bind.to_string())
                            .or_default()
                            .push(value.clone());
                    }
                    let preference = self.channel(channel_id)?.preference;
                    let preferred = preferred_continuation(preference, &sender.tasklet, "main");
                    self.emit_transfer(
                        channel_id,
                        &sender.tasklet,
                        "main",
                        &sender.payload,
                        preferred,
                    );
                    if preference >= 1 {
                        self.switch("main", &sender.tasklet, "channel_preference_sender");
                        self.current = sender.tasklet.clone();
                        match self.run_tasklet_chain(&sender.tasklet, None)? {
                            TaskletOutcome::Blocked | TaskletOutcome::Paused => {}
                            TaskletOutcome::Complete(done) => {
                                self.switch(&done, "main", "run_complete");
                                self.current = String::from("main");
                            }
                        }
                    } else {
                        self.schedule_new_tasklet(&sender.tasklet)?;
                    }
                    return Ok(());
                }
                TransferPayload::Exception { .. } | TransferPayload::Throw { .. } => {
                    let error = sender.payload.as_error_json();
                    self.error = Some(error.clone());
                    let preference = self.channel(channel_id)?.preference;
                    let preferred = preferred_continuation(preference, &sender.tasklet, "main");
                    self.emit_transfer(
                        channel_id,
                        &sender.tasklet,
                        "main",
                        &sender.payload,
                        preferred,
                    );
                    self.emit(
                        "channel.exception",
                        [
                            ("actor", json!("main")),
                            ("channel", json!(channel_id)),
                            ("receiver", json!("main")),
                            ("error", error),
                        ],
                    );
                    if preference >= 1 {
                        self.schedule_new_tasklet(&sender.tasklet)?;
                    }
                    return Ok(());
                }
            }
        }

        if self.main_block_trap {
            self.main_channel_error(
                channel_id,
                "RuntimeError",
                "Channel cannot block on main tasklet with block_trap set true",
                "receive",
            )?;
        } else if self.channel(channel_id)?.closed || self.channel(channel_id)?.closing {
            self.main_channel_error(
                channel_id,
                "ValueError",
                "receive operation on a closed channel",
                "receive",
            )?;
        } else {
            let tasklets = self.runnable_tasklets();
            if !tasklets.is_empty() {
                self.emit(
                    "scheduler.drain_before_block",
                    [
                        ("actor", json!("main")),
                        ("reason", json!("main_receive_would_block")),
                        ("tasklets", json!(tasklets.clone())),
                    ],
                );
                self.batch_complete_append_tasklets(&tasklets)?;
            }

            let error = json!({
                "type": "RuntimeError",
                "message_contains": "Deadlock"
            });
            self.error = Some(error.clone());
            self.emit(
                "scheduler.deadlock",
                [
                    ("actor", json!("main")),
                    ("operation", json!("receive")),
                    ("channel", json!(channel_id)),
                    ("error", error),
                    ("main_blocked", json!(false)),
                    ("balance", json!(self.channel(channel_id)?.balance())),
                ],
            );
        }

        Ok(())
    }

    fn main_send_entry(&mut self, channel_id: &str, value: Value) -> Result<(), SchedulerError> {
        self.main_send_payload(channel_id, TransferPayload::Value(value))
    }

    fn main_send_payload(
        &mut self,
        channel_id: &str,
        payload: TransferPayload,
    ) -> Result<(), SchedulerError> {
        self.emit_channel_callback(
            channel_id,
            "main",
            true,
            self.channel(channel_id)?.blocked_receivers.is_empty(),
        );

        let waiting_receivers = self
            .channel(channel_id)?
            .blocked_receivers
            .iter()
            .map(|receiver| receiver.tasklet.clone())
            .collect::<Vec<_>>();
        self.emit_send_attempt("main", channel_id, &payload, waiting_receivers);

        let blocked_receiver = self.channel_mut(channel_id)?.blocked_receivers.pop_front();
        if let Some(receiver) = blocked_receiver {
            self.unblock_tasklet(&receiver.tasklet)?;
            let receiver_raised =
                self.deliver_payload_to_tasklet(&receiver.tasklet, receiver.bind, &payload)?;
            self.advance_pc(&receiver.tasklet)?;

            let preference = self.channel(channel_id)?.preference;
            let preferred = preferred_continuation(preference, "main", &receiver.tasklet);
            self.update_channel_close_state(channel_id)?;
            self.emit_transfer(channel_id, "main", &receiver.tasklet, &payload, preferred);

            if receiver_raised {
                self.tasklet_error_from_receive(&receiver.tasklet)?;
            }

            if preference <= -1 && !receiver_raised {
                self.switch("main", &receiver.tasklet, "channel_preference_receiver");
                self.current = receiver.tasklet.clone();
                match self.run_tasklet_chain(&receiver.tasklet, None)? {
                    TaskletOutcome::Blocked | TaskletOutcome::Paused => {}
                    TaskletOutcome::Complete(done) => {
                        self.switch(&done, "main", "run_complete");
                        self.current = String::from("main");
                    }
                }
            } else {
                if !receiver_raised {
                    self.schedule_new_tasklet(&receiver.tasklet)?;
                }
            }
        } else if self.main_block_trap {
            self.main_channel_error(
                channel_id,
                "RuntimeError",
                "Channel cannot block on main tasklet with block_trap set true",
                "send",
            )?;
        } else if self.channel(channel_id)?.closed || self.channel(channel_id)?.closing {
            self.main_channel_error(
                channel_id,
                "ValueError",
                "Send operation on a closed channel",
                "send",
            )?;
        } else {
            let tasklets = self.runnable_tasklets();
            if !tasklets.is_empty() {
                self.emit(
                    "scheduler.drain_before_block",
                    [
                        ("actor", json!("main")),
                        ("reason", json!("main_send_would_block")),
                        ("tasklets", json!(tasklets.clone())),
                    ],
                );
                self.batch_complete_append_tasklets(&tasklets)?;
            }

            let error = json!({
                "type": "RuntimeError",
                "message_contains": "Deadlock"
            });
            self.error = Some(error.clone());
            self.emit(
                "scheduler.deadlock",
                [
                    ("actor", json!("main")),
                    ("operation", json!("send")),
                    ("channel", json!(channel_id)),
                    ("error", error),
                    ("main_blocked", json!(false)),
                    ("balance", json!(self.channel(channel_id)?.balance())),
                ],
            );
        }

        Ok(())
    }

    fn bind_tasklet(
        &mut self,
        actor: &str,
        tasklet: &str,
        body: Option<Vec<Operation>>,
        args_bound: bool,
    ) -> Result<(), SchedulerError> {
        let state = self.tasklet(tasklet)?;
        if state.scheduled {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "bind",
                reason: "scheduled",
            });
        }
        if state.blocked {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "bind",
                reason: "blocked",
            });
        }
        if body.is_none() && !state.callable_bound {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "bind",
                reason: "unbound",
            });
        }

        let state = self.tasklet_mut(tasklet)?;
        if let Some(body) = body {
            state.body = body;
            state.callable_bound = true;
        }
        state.pc = 0;
        state.alive = args_bound;
        state.scheduled = false;
        state.blocked = false;
        state.paused = args_bound;
        state.pending_exit = false;
        state.raised = None;
        state.blocked_on = None;
        state.blocked_direction = None;
        state.resume_after_completion = None;
        state.times_switched_to = 0;

        self.emit(
            "tasklet.bind",
            [
                ("actor", json!(actor)),
                ("tasklet", json!(tasklet)),
                ("callable_bound", json!(true)),
                ("args_bound", json!(args_bound)),
                ("alive", json!(args_bound)),
                ("scheduled", json!(false)),
                ("paused", json!(args_bound)),
                ("times_switched_to", json!(0)),
                ("run_count", json!(self.run_count())),
            ],
        );
        Ok(())
    }

    fn setup_tasklet(&mut self, actor: &str, tasklet: &str) -> Result<(), SchedulerError> {
        let state = self.tasklet(tasklet)?;
        if !state.callable_bound {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "setup",
                reason: "unbound",
            });
        }
        if state.scheduled {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "setup",
                reason: "scheduled",
            });
        }
        if state.blocked {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "setup",
                reason: "blocked",
            });
        }

        {
            let state = self.tasklet_mut(tasklet)?;
            state.pc = 0;
            state.alive = true;
            state.paused = false;
            state.pending_exit = false;
            state.raised = None;
            state.resume_after_completion = None;
        }
        self.schedule_new_tasklet(tasklet)?;
        self.emit(
            "tasklet.setup",
            [
                ("actor", json!(actor)),
                ("tasklet", json!(tasklet)),
                ("alive", json!(true)),
                ("scheduled", json!(true)),
                ("paused", json!(false)),
                ("run_count", json!(self.run_count())),
                ("runnable", json!(self.runnable_snapshot())),
            ],
        );
        Ok(())
    }

    fn unbind_tasklet(&mut self, actor: &str, tasklet: &str) -> Result<(), SchedulerError> {
        self.tasklet(tasklet)?;
        if actor == tasklet {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "unbind",
                reason: "current",
            });
        }
        if self.tasklet(tasklet)?.scheduled {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "unbind",
                reason: "scheduled",
            });
        }

        self.remove_from_runnable(tasklet);
        let state = self.tasklet_mut(tasklet)?;
        state.callable_bound = false;
        state.body.clear();
        state.pc = 0;
        state.alive = false;
        state.scheduled = false;
        state.blocked = false;
        state.paused = false;
        state.pending_exit = false;
        state.raised = None;
        state.blocked_on = None;
        state.blocked_direction = None;
        state.resume_after_completion = None;

        self.emit(
            "tasklet.unbind",
            [
                ("actor", json!(actor)),
                ("tasklet", json!(tasklet)),
                ("callable_bound", json!(false)),
                ("alive", json!(false)),
                ("scheduled", json!(false)),
                ("run_count", json!(self.run_count())),
            ],
        );
        Ok(())
    }

    fn remove_tasklet(&mut self, actor: &str, tasklet: &str) -> Result<(), SchedulerError> {
        self.tasklet(tasklet)?;
        self.remove_from_runnable(tasklet);
        let (alive, paused) = {
            let state = self.tasklet_mut(tasklet)?;
            state.scheduled = false;
            state.blocked = false;
            state.paused = state.alive;
            state.blocked_on = None;
            state.blocked_direction = None;
            (state.alive, state.paused)
        };
        self.emit(
            "tasklet.remove",
            [
                ("actor", json!(actor)),
                ("tasklet", json!(tasklet)),
                ("alive", json!(alive)),
                ("scheduled", json!(false)),
                ("paused", json!(paused)),
                ("run_count", json!(self.run_count())),
            ],
        );
        Ok(())
    }

    fn insert_tasklet(&mut self, actor: &str, tasklet: &str) -> Result<(), SchedulerError> {
        let state = self.tasklet(tasklet)?;
        if !state.callable_bound {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "insert",
                reason: "unbound",
            });
        }
        if state.blocked {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "insert",
                reason: "blocked",
            });
        }
        if !state.alive {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation: "insert",
                reason: "dead",
            });
        }

        self.schedule_new_tasklet(tasklet)?;
        self.tasklet_mut(tasklet)?.paused = false;
        self.emit(
            "tasklet.insert",
            [
                ("actor", json!(actor)),
                ("tasklet", json!(tasklet)),
                ("alive", json!(true)),
                ("scheduled", json!(true)),
                ("paused", json!(false)),
                ("run_count", json!(self.run_count())),
                ("runnable", json!(self.runnable_snapshot())),
            ],
        );
        Ok(())
    }

    fn kill_tasklet(
        &mut self,
        actor: &str,
        tasklet: &str,
        pending: bool,
    ) -> Result<(), SchedulerError> {
        let alive = self.tasklet(tasklet)?.alive;
        if !alive {
            self.emit(
                "tasklet.kill",
                [
                    ("actor", json!(actor)),
                    ("tasklet", json!(tasklet)),
                    ("pending", json!(pending)),
                    ("alive", json!(false)),
                    ("noop", json!(true)),
                    ("run_count", json!(self.run_count())),
                ],
            );
            return Ok(());
        }

        self.unblock_tasklet_from_channels(tasklet)?;
        if pending {
            let state = self.tasklet_mut(tasklet)?;
            state.pending_exit = true;
            state.paused = false;
            state.blocked = false;
            state.blocked_on = None;
            state.blocked_direction = None;
            self.schedule_new_tasklet(tasklet)?;
            self.emit(
                "tasklet.kill",
                [
                    ("actor", json!(actor)),
                    ("tasklet", json!(tasklet)),
                    ("pending", json!(true)),
                    ("alive", json!(true)),
                    ("scheduled", json!(true)),
                    ("pending_exit", json!(true)),
                    ("run_count", json!(self.run_count())),
                    ("runnable", json!(self.runnable_snapshot())),
                ],
            );
        } else {
            self.remove_from_runnable(tasklet);
            let state = self.tasklet_mut(tasklet)?;
            state.alive = false;
            state.scheduled = false;
            state.blocked = false;
            state.paused = false;
            state.pending_exit = false;
            state.blocked_on = None;
            state.blocked_direction = None;
            self.emit(
                "tasklet.kill",
                [
                    ("actor", json!(actor)),
                    ("tasklet", json!(tasklet)),
                    ("pending", json!(false)),
                    ("alive", json!(false)),
                    ("scheduled", json!(false)),
                    ("run_count", json!(self.run_count())),
                ],
            );
        }
        Ok(())
    }

    fn raise_exception_on_tasklet(
        &mut self,
        tasklet: &str,
        exception: &str,
        actor: &str,
    ) -> Result<(), SchedulerError> {
        if exception == "TaskletExit" {
            return self.kill_tasklet(actor, tasklet, false);
        }
        self.unblock_tasklet_from_channels(tasklet)?;
        self.tasklet_mut(tasklet)?.raised = Some(json!({
            "type": exception,
        }));
        self.schedule_new_tasklet(tasklet)?;
        self.emit(
            "tasklet.raise_exception",
            [
                ("actor", json!(actor)),
                ("tasklet", json!(tasklet)),
                ("exception", json!(exception)),
                ("scheduled", json!(true)),
            ],
        );
        Ok(())
    }

    fn unblock_tasklet_from_channels(&mut self, tasklet: &str) -> Result<(), SchedulerError> {
        let mut affected_channels = Vec::new();
        for (channel_id, channel) in self.channels.iter_mut() {
            let senders_before = channel.blocked_senders.len();
            let receivers_before = channel.blocked_receivers.len();
            channel
                .blocked_senders
                .retain(|sender| sender.tasklet != tasklet);
            channel
                .blocked_receivers
                .retain(|receiver| receiver.tasklet != tasklet);
            if channel.blocked_senders.len() != senders_before
                || channel.blocked_receivers.len() != receivers_before
            {
                affected_channels.push(channel_id.clone());
            }
        }
        for channel_id in affected_channels {
            self.update_channel_close_state(&channel_id)?;
        }
        self.unblock_tasklet(tasklet)
    }

    fn switch_tasklet_entry(&mut self, tasklet: &str) -> Result<(), SchedulerError> {
        self.ensure_tasklet_can_run(tasklet, "switch")?;
        if self.reject_if_switch_trapped("main", "tasklet.switch") {
            return Ok(());
        }
        let from = self.current.clone();
        self.switch(&from, tasklet, "switch_tasklet");
        self.current = tasklet.to_string();
        self.remove_from_runnable(tasklet);
        self.set_scheduled(tasklet, false)?;
        match self.run_tasklet_chain(tasklet, None)? {
            TaskletOutcome::Blocked | TaskletOutcome::Paused => {}
            TaskletOutcome::Complete(done) => {
                self.switch(&done, "main", "run_complete");
                self.current = String::from("main");
            }
        }
        Ok(())
    }

    fn switch_tasklet_operation(
        &mut self,
        caller: &str,
        target: &str,
    ) -> Result<StepOutcome, SchedulerError> {
        self.ensure_tasklet_can_run(target, "switch")?;

        self.advance_pc(caller)?;
        let nested_tail = self.direct_run_nested_tail(target)?;
        self.remove_from_runnable(target);
        self.set_scheduled(target, false)?;
        self.tasklet_mut(caller)?.paused = true;
        self.switch(caller, target, "switch_tasklet");
        self.current = target.to_string();

        match self.run_tasklet_chain(target, None)? {
            TaskletOutcome::Blocked => Ok(StepOutcome::Blocked),
            TaskletOutcome::Paused => Ok(StepOutcome::Continue),
            TaskletOutcome::Complete(done) => {
                let reason =
                    if self.scenario.nested_tasklets && self.has_runnable_from(&nested_tail) {
                        "tasklet_complete"
                    } else if !self.scenario.nested_tasklets && self.has_runnable_tasklets() {
                        "tasklet_complete"
                    } else {
                        "run_complete"
                    };
                self.switch(&done, "main", reason);
                self.current = String::from("main");
                if self.scenario.nested_tasklets {
                    self.run_nested_tail(&nested_tail)?;
                } else {
                    self.run_remaining_after_direct_tasklet_run()?;
                }
                let from = self.current.clone();
                self.switch(&from, caller, "resume_tasklet_switcher");
                self.current = caller.to_string();
                self.tasklet_mut(caller)?.paused = false;
                Ok(StepOutcome::Continue)
            }
        }
    }

    fn batch_complete_append_tasklets(
        &mut self,
        tasklets: &[String],
    ) -> Result<(), SchedulerError> {
        let mut appended_target: Option<String> = None;
        for tasklet in tasklets {
            self.remove_from_runnable(tasklet);
            let body = self.tasklet(tasklet)?.body.clone();
            for op in body {
                if let Operation::Append { target, value } = op {
                    appended_target.get_or_insert_with(|| target.clone());
                    self.observations.entry(target).or_default().push(value);
                }
            }
            self.complete_tasklet_without_event(tasklet)?;
        }

        let mut fields = vec![
            (String::from("actor"), json!("main")),
            (String::from("tasklets"), json!(tasklets)),
            (String::from("run_count"), json!(self.run_count())),
        ];
        if let Some(target) = appended_target {
            if let Some(values) = self.observations.get(&target) {
                fields.push((target, json!(values)));
            }
        }
        self.emit("tasklet.batch_complete", fields);
        Ok(())
    }

    fn complete_tasklet(&mut self, tasklet: &str) -> Result<(), SchedulerError> {
        self.complete_tasklet_without_event(tasklet)?;
        self.emit(
            "tasklet.complete",
            [
                ("actor", json!(tasklet)),
                ("tasklet", json!(tasklet)),
                ("alive", json!(false)),
            ],
        );
        Ok(())
    }

    fn complete_tasklet_without_event(&mut self, tasklet: &str) -> Result<(), SchedulerError> {
        let state = self.tasklet_mut(tasklet)?;
        state.alive = false;
        state.scheduled = false;
        state.blocked = false;
        state.paused = false;
        state.pending_exit = false;
        state.raised = None;
        state.blocked_on = None;
        state.blocked_direction = None;
        state.pc = state.body.len();
        Ok(())
    }

    fn emit_channel_callback(
        &mut self,
        channel_id: &str,
        actor: &str,
        is_sending: bool,
        will_block: bool,
    ) {
        if !self.scenario.channel_callbacks {
            return;
        }
        let callback = json!({
            "channel": channel_id,
            "tasklet": actor,
            "is_sending": is_sending,
            "will_block": will_block,
            "balance": self.channels[channel_id].balance()
        });
        self.observations
            .entry(String::from("channel_callbacks"))
            .or_default()
            .push(callback.clone());
        self.emit(
            "channel.callback",
            [
                ("actor", json!(actor)),
                ("channel", json!(channel_id)),
                ("is_sending", json!(is_sending)),
                ("will_block", json!(will_block)),
                ("balance", json!(self.channels[channel_id].balance())),
                ("callback", callback),
            ],
        );
    }

    fn emit_send_attempt(
        &mut self,
        sender: &str,
        channel_id: &str,
        payload: &TransferPayload,
        waiting_receivers: Vec<String>,
    ) {
        self.emit(
            "channel.send.attempt",
            [
                ("actor", json!(sender)),
                ("channel", json!(channel_id)),
                ("value", payload.trace_value()),
                ("payload_kind", json!(payload.kind())),
                ("waiting_receivers", json!(waiting_receivers)),
            ],
        );
    }

    fn deliver_payload_to_tasklet(
        &mut self,
        receiver: &str,
        bind: Option<String>,
        payload: &TransferPayload,
    ) -> Result<bool, SchedulerError> {
        match payload {
            TransferPayload::Value(value) => {
                if let Some(bind) = bind {
                    self.tasklet_mut(receiver)?.vars.insert(bind, value.clone());
                }
                Ok(false)
            }
            TransferPayload::Exception { .. } | TransferPayload::Throw { .. } => {
                self.tasklet_mut(receiver)?.raised = Some(payload.as_error_json());
                Ok(true)
            }
        }
    }

    fn tasklet_error_from_receive(&mut self, tasklet: &str) -> Result<(), SchedulerError> {
        let error = self
            .tasklet(tasklet)?
            .raised
            .clone()
            .unwrap_or_else(|| json!({"type": "RuntimeError"}));
        self.emit(
            "tasklet.exception",
            [
                ("actor", json!(tasklet)),
                ("tasklet", json!(tasklet)),
                ("error", error.clone()),
            ],
        );
        self.complete_tasklet_without_event(tasklet)?;
        self.tasklet_mut(tasklet)?.raised = Some(error);
        Ok(())
    }

    fn tasklet_channel_error(
        &mut self,
        tasklet: &str,
        channel_id: &str,
        error_type: &str,
        message: &str,
    ) -> Result<(), SchedulerError> {
        let error = json!({
            "type": error_type,
            "message_contains": message
        });
        self.tasklet_mut(tasklet)?.raised = Some(error.clone());
        self.advance_pc(tasklet)?;
        self.emit(
            "channel.error",
            [
                ("actor", json!(tasklet)),
                ("channel", json!(channel_id)),
                ("error", error),
            ],
        );
        Ok(())
    }

    fn main_channel_error(
        &mut self,
        channel_id: &str,
        error_type: &str,
        message: &str,
        operation: &str,
    ) -> Result<(), SchedulerError> {
        let error = json!({
            "type": error_type,
            "message_contains": message
        });
        self.error = Some(error.clone());
        self.emit(
            "channel.error",
            [
                ("actor", json!("main")),
                ("operation", json!(operation)),
                ("channel", json!(channel_id)),
                ("error", error),
            ],
        );
        Ok(())
    }

    fn update_channel_close_state(&mut self, channel_id: &str) -> Result<(), SchedulerError> {
        let should_close = {
            let channel = self.channel(channel_id)?;
            channel.closing && channel.balance() == 0
        };
        if should_close {
            let channel = self.channel_mut(channel_id)?;
            channel.closed = true;
        }
        Ok(())
    }

    fn close_channel(&mut self, actor: &str, channel_id: &str) -> Result<(), SchedulerError> {
        {
            let channel = self.channel_mut(channel_id)?;
            channel.closing = true;
            if channel.balance() == 0 {
                channel.closed = true;
            }
        }
        self.emit(
            "channel.close",
            [
                ("actor", json!(actor)),
                ("channel", json!(channel_id)),
                ("balance", json!(self.channel(channel_id)?.balance())),
                ("closed", json!(self.channel(channel_id)?.closed)),
                ("closing", json!(self.channel(channel_id)?.closing)),
            ],
        );
        Ok(())
    }

    fn open_channel(&mut self, actor: &str, channel_id: &str) -> Result<(), SchedulerError> {
        {
            let channel = self.channel_mut(channel_id)?;
            channel.closed = false;
            channel.closing = false;
        }
        self.emit(
            "channel.open",
            [
                ("actor", json!(actor)),
                ("channel", json!(channel_id)),
                ("closed", json!(false)),
            ],
        );
        Ok(())
    }

    fn clear_channel(
        &mut self,
        actor: &str,
        channel_id: &str,
        pending: bool,
    ) -> Result<(), SchedulerError> {
        let (receivers, senders) = {
            let channel = self.channel_mut(channel_id)?;
            let receivers = channel
                .blocked_receivers
                .drain(..)
                .map(|receiver| receiver.tasklet)
                .collect::<Vec<_>>();
            let senders = channel
                .blocked_senders
                .drain(..)
                .map(|sender| sender.tasklet)
                .collect::<Vec<_>>();
            (receivers, senders)
        };
        for tasklet in receivers.iter().chain(senders.iter()) {
            self.unblock_tasklet(tasklet)?;
            if pending {
                self.schedule_new_tasklet(tasklet)?;
                self.tasklet_mut(tasklet)?.pending_exit = true;
            } else {
                self.complete_tasklet_without_event(tasklet)?;
            }
        }
        self.update_channel_close_state(channel_id)?;
        let killed_tasklets = receivers
            .iter()
            .chain(senders.iter())
            .cloned()
            .collect::<Vec<_>>();
        self.emit(
            "channel.clear",
            [
                ("actor", json!(actor)),
                ("channel", json!(channel_id)),
                ("receivers", json!(receivers)),
                ("senders", json!(senders)),
                ("pending", json!(pending)),
                ("killed_tasklets", json!(killed_tasklets)),
                ("balance", json!(self.channel(channel_id)?.balance())),
                ("closed", json!(self.channel(channel_id)?.closed)),
                ("closing", json!(self.channel(channel_id)?.closing)),
            ],
        );
        Ok(())
    }

    fn probe_channel_queue(
        &mut self,
        actor: &str,
        channel_id: &str,
        target: Option<&str>,
    ) -> Result<(), SchedulerError> {
        let queue_front = self
            .channel(channel_id)?
            .blocked_receivers
            .front()
            .map(|receiver| receiver.tasklet.clone())
            .or_else(|| {
                self.channel(channel_id)
                    .ok()
                    .and_then(|channel| channel.blocked_senders.front())
                    .map(|sender| sender.tasklet.clone())
            });
        if let (Some(target), Some(queue_front)) = (target, queue_front.clone()) {
            self.observations
                .entry(target.to_string())
                .or_default()
                .push(json!(queue_front));
        }
        self.emit(
            "channel.queue_front",
            [
                ("actor", json!(actor)),
                ("channel", json!(channel_id)),
                ("front", json!(queue_front.clone())),
                ("tasklet", json!(queue_front)),
                ("blocked_senders", json!(self.blocked_senders(channel_id)?)),
                (
                    "blocked_receivers",
                    json!(self.blocked_receivers(channel_id)?),
                ),
            ],
        );
        Ok(())
    }

    fn emit_transfer(
        &mut self,
        channel_id: &str,
        sender: &str,
        receiver: &str,
        payload: &TransferPayload,
        preferred: &str,
    ) {
        let preference = self.channels[channel_id].preference;
        let balance = self.channels[channel_id].balance();
        self.emit(
            "channel.transfer",
            [
                ("actor", json!(sender)),
                ("channel", json!(channel_id)),
                ("sender", json!(sender)),
                ("receiver", json!(receiver)),
                ("value", payload.trace_value()),
                ("payload_kind", json!(payload.kind())),
                ("balance", json!(balance)),
                ("preference", json!(preference)),
                ("preferred_continuation", json!(preferred)),
            ],
        );
    }

    fn switch(&mut self, from: &str, to: &str, reason: &str) {
        let to_times_switched_to = if let Some(tasklet) = self.tasklets.get_mut(to) {
            tasklet.times_switched_to += 1;
            Some(tasklet.times_switched_to)
        } else {
            None
        };
        let mut fields = vec![
            (String::from("actor"), json!(from)),
            (String::from("from"), json!(from)),
            (String::from("to"), json!(to)),
            (String::from("reason"), json!(reason)),
        ];
        if let Some(times) = to_times_switched_to {
            fields.push((String::from("to_times_switched_to"), json!(times)));
        }
        self.emit("scheduler.switch", fields);
    }

    fn emit<K, I>(&mut self, kind: &str, fields: I)
    where
        K: Into<String>,
        I: IntoIterator<Item = (K, Value)>,
    {
        let mut event = Map::new();
        event.insert(String::from("seq"), json!(self.seq));
        event.insert(String::from("kind"), json!(kind));
        for (key, value) in fields {
            event.insert(key.into(), value);
        }
        self.events.push(Value::Object(event));
        self.seq += 1;
    }

    fn final_state(&self) -> Value {
        let mut root = Map::new();
        root.insert(String::from("current"), json!(self.current));
        root.insert(String::from("run_count"), json!(self.run_count()));
        root.insert(
            String::from("switch_trap_level"),
            json!(self.switch_trap_level),
        );
        root.insert(
            String::from("last_timeout_completed_tasklets"),
            json!(self.last_timeout_completed_tasklets),
        );
        root.insert(
            String::from("last_timeout_switched_tasklets"),
            json!(self.last_timeout_switched_tasklets),
        );
        for (target, values) in &self.observations {
            root.insert(target.clone(), json!(values));
        }
        if let Some(error) = &self.error {
            root.insert(String::from("error"), error.clone());
        }

        let mut tasklets = Map::new();
        tasklets.insert(
            String::from("main"),
            json!({
                "callable_bound": true,
                "alive": true,
                "blocked": false,
                "scheduled": true,
                "paused": false,
                "pending_exit": false,
                "block_trap": self.main_block_trap,
                "times_switched_to": 1
            }),
        );
        for (id, state) in &self.tasklets {
            let mut tasklet = Map::new();
            tasklet.insert(String::from("callable_bound"), json!(state.callable_bound));
            tasklet.insert(String::from("alive"), json!(state.alive));
            tasklet.insert(String::from("scheduled"), json!(state.scheduled));
            tasklet.insert(String::from("blocked"), json!(state.blocked));
            tasklet.insert(String::from("paused"), json!(state.paused));
            tasklet.insert(String::from("pending_exit"), json!(state.pending_exit));
            tasklet.insert(String::from("block_trap"), json!(state.block_trap));
            tasklet.insert(
                String::from("times_switched_to"),
                json!(state.times_switched_to),
            );
            if let Some(raised) = &state.raised {
                tasklet.insert(String::from("raised"), raised.clone());
            }
            if let Some(blocked_on) = &state.blocked_on {
                tasklet.insert(String::from("blocked_on"), json!(blocked_on));
            }
            if let Some(direction) = state.blocked_direction {
                tasklet.insert(String::from("blocked_direction"), json!(direction.as_str()));
            }
            for (key, value) in &state.vars {
                tasklet.insert(key.clone(), value.clone());
            }
            tasklets.insert(id.clone(), Value::Object(tasklet));
        }
        root.insert(String::from("tasklets"), Value::Object(tasklets));

        let mut channels = Map::new();
        for (id, channel) in &self.channels {
            let queue_front = channel
                .blocked_receivers
                .front()
                .map(|receiver| receiver.tasklet.clone())
                .or_else(|| {
                    channel
                        .blocked_senders
                        .front()
                        .map(|sender| sender.tasklet.clone())
                });
            channels.insert(
                id.clone(),
                json!({
                    "balance": channel.balance(),
                    "closed": channel.closed,
                    "closing": channel.closing,
                    "queue_front": queue_front,
                    "blocked_senders": channel.blocked_senders.iter().map(|sender| sender.tasklet.clone()).collect::<Vec<_>>(),
                    "blocked_receivers": channel.blocked_receivers.iter().map(|receiver| receiver.tasklet.clone()).collect::<Vec<_>>(),
                }),
            );
        }
        root.insert(String::from("channels"), Value::Object(channels));
        root.insert(
            String::from("all_time_tasklet_count"),
            json!(self.tasklets.len() + 1),
        );
        root.insert(
            String::from("active_tasklet_count"),
            json!(
                self.tasklets
                    .values()
                    .filter(|tasklet| tasklet.alive)
                    .count()
                    + 1
            ),
        );

        Value::Object(root)
    }

    fn uses_batch_new_for_main_deadlock(&self) -> bool {
        matches!(
            self.scenario.entrypoint,
            Entrypoint::Receive {
                ref actor,
                ..
            } if actor == "main"
        )
    }

    fn scenario_tasklet_ids(&self) -> Vec<String> {
        self.scenario
            .tasklets
            .iter()
            .map(|tasklet| tasklet.id.clone())
            .collect()
    }

    fn runnable_snapshot(&self) -> Vec<String> {
        self.runnable.iter().cloned().collect()
    }

    fn runnable_tasklets(&self) -> Vec<String> {
        self.runnable
            .iter()
            .filter(|id| id.as_str() != "main")
            .cloned()
            .collect()
    }

    fn direct_run_nested_tail(&self, tasklet: &str) -> Result<Vec<String>, SchedulerError> {
        if !self.scenario.nested_tasklets {
            return Ok(Vec::new());
        }
        let target_depth = self.tasklet(tasklet)?.depth;
        let Some(index) = self.runnable.iter().position(|id| id == tasklet) else {
            return Ok(Vec::new());
        };

        let mut tail = Vec::new();
        for id in self.runnable.iter().skip(index + 1) {
            if id == "main" {
                continue;
            }
            if self.tasklet(id)?.depth < target_depth {
                break;
            }
            tail.push(id.clone());
        }
        Ok(tail)
    }

    fn has_runnable_from(&self, tasklets: &[String]) -> bool {
        tasklets.iter().any(|id| self.is_runnable_tasklet(id))
    }

    fn is_runnable_tasklet(&self, tasklet: &str) -> bool {
        self.runnable.iter().any(|queued| queued == tasklet)
    }

    fn run_count(&self) -> usize {
        self.runnable.len()
    }

    fn has_runnable_tasklets(&self) -> bool {
        self.runnable.iter().any(|id| id != "main")
    }

    fn pop_next_runnable_tasklet(&mut self) -> Option<String> {
        let index = self.runnable.iter().position(|id| id != "main")?;
        let id = self.runnable.remove(index)?;
        let _ = self.set_scheduled(&id, false);
        Some(id)
    }

    fn remove_from_runnable(&mut self, id: &str) {
        if let Some(index) = self.runnable.iter().position(|queued| queued == id) {
            self.runnable.remove(index);
        }
    }

    fn schedule_new_tasklet(&mut self, id: &str) -> Result<(), SchedulerError> {
        if !self.runnable.iter().any(|queued| queued == id) {
            self.runnable.push_back(id.to_string());
        }
        self.set_scheduled(id, true)?;
        self.tasklet_mut(id)?.paused = false;
        Ok(())
    }

    fn spawn_tasklet(&mut self, actor: &str, tasklet: String) -> Result<(), SchedulerError> {
        let actor_depth = self.tasklet(actor)?.depth;
        let child_depth = if self.scenario.nested_tasklets {
            actor_depth + 1
        } else {
            0
        };
        self.tasklet_mut(&tasklet)?.depth = child_depth;
        self.insert_spawned_tasklet(&tasklet, child_depth)?;
        self.set_scheduled(&tasklet, true)?;
        self.emit(
            "tasklet.new",
            [
                ("actor", json!(actor)),
                ("tasklet", json!(tasklet)),
                ("alive", json!(true)),
                ("scheduled", json!(true)),
                ("run_count", json!(self.run_count())),
                ("runnable", json!(self.runnable_snapshot())),
            ],
        );
        Ok(())
    }

    fn insert_spawned_tasklet(
        &mut self,
        tasklet: &str,
        depth: usize,
    ) -> Result<(), SchedulerError> {
        self.remove_from_runnable(tasklet);
        if !self.scenario.nested_tasklets {
            self.runnable.push_back(tasklet.to_string());
            return Ok(());
        }

        let insert_at = self
            .runnable
            .iter()
            .enumerate()
            .find_map(|(index, id)| {
                if id == "main" {
                    return None;
                }
                match self.tasklet(id) {
                    Ok(state) if state.depth < depth => Some(index),
                    _ => None,
                }
            })
            .unwrap_or(self.runnable.len());
        self.runnable.insert(insert_at, tasklet.to_string());
        Ok(())
    }

    fn block_tasklet(
        &mut self,
        tasklet: &str,
        channel: &str,
        direction: BlockDirection,
    ) -> Result<(), SchedulerError> {
        self.remove_from_runnable(tasklet);
        let state = self.tasklet_mut(tasklet)?;
        state.scheduled = false;
        state.blocked = true;
        state.paused = false;
        state.blocked_on = Some(channel.to_string());
        state.blocked_direction = Some(direction);
        Ok(())
    }

    fn unblock_tasklet(&mut self, tasklet: &str) -> Result<(), SchedulerError> {
        let state = self.tasklet_mut(tasklet)?;
        state.scheduled = false;
        state.blocked = false;
        state.paused = false;
        state.blocked_on = None;
        state.blocked_direction = None;
        Ok(())
    }

    fn set_scheduled(&mut self, tasklet: &str, scheduled: bool) -> Result<(), SchedulerError> {
        self.tasklet_mut(tasklet)?.scheduled = scheduled;
        Ok(())
    }

    fn set_main_block_trap(&mut self, value: bool) {
        self.main_block_trap = value;
        self.emit(
            "tasklet.block_trap",
            [
                ("actor", json!("main")),
                ("tasklet", json!("main")),
                ("block_trap", json!(value)),
                ("run_count", json!(self.run_count())),
            ],
        );
    }

    fn set_switch_trap(&mut self, delta: i64) {
        let previous = self.switch_trap_level;
        self.switch_trap_level += delta;
        self.emit(
            "scheduler.switch_trap",
            [
                ("actor", json!(self.current)),
                ("delta", json!(delta)),
                ("previous", json!(previous)),
                ("switch_trap_level", json!(self.switch_trap_level)),
            ],
        );
    }

    fn reject_if_switch_trapped(&mut self, actor: &str, operation: &str) -> bool {
        if self.switch_trap_level == 0 {
            return false;
        }
        let error = json!({
            "type": "RuntimeError",
            "message_contains": "switch_trap"
        });
        self.error = Some(error.clone());
        self.emit(
            "scheduler.switch_trap_error",
            [
                ("actor", json!(actor)),
                ("operation", json!(operation)),
                ("switch_trap_level", json!(self.switch_trap_level)),
                ("error", error),
            ],
        );
        true
    }

    fn set_tasklet_block_trap(&mut self, tasklet: &str, value: bool) -> Result<(), SchedulerError> {
        self.tasklet_mut(tasklet)?.block_trap = value;
        self.emit(
            "tasklet.block_trap",
            [
                ("actor", json!(tasklet)),
                ("tasklet", json!(tasklet)),
                ("block_trap", json!(value)),
                ("run_count", json!(self.run_count())),
            ],
        );
        Ok(())
    }

    fn ensure_tasklet_can_run(
        &self,
        tasklet: &str,
        operation: &'static str,
    ) -> Result<(), SchedulerError> {
        let state = self.tasklet(tasklet)?;
        if !state.callable_bound {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation,
                reason: "unbound",
            });
        }
        if !state.alive {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation,
                reason: "dead",
            });
        }
        if state.blocked {
            return Err(SchedulerError::InvalidTaskletOperation {
                tasklet: tasklet.to_string(),
                operation,
                reason: "blocked",
            });
        }
        Ok(())
    }

    fn advance_pc(&mut self, tasklet: &str) -> Result<(), SchedulerError> {
        self.tasklet_mut(tasklet)?.pc += 1;
        Ok(())
    }

    fn blocked_senders(&self, channel: &str) -> Result<Vec<String>, SchedulerError> {
        Ok(self
            .channel(channel)?
            .blocked_senders
            .iter()
            .map(|sender| sender.tasklet.clone())
            .collect())
    }

    fn blocked_receivers(&self, channel: &str) -> Result<Vec<String>, SchedulerError> {
        Ok(self
            .channel(channel)?
            .blocked_receivers
            .iter()
            .map(|receiver| receiver.tasklet.clone())
            .collect())
    }

    fn tasklet(&self, id: &str) -> Result<&TaskletState, SchedulerError> {
        self.tasklets
            .get(id)
            .ok_or_else(|| SchedulerError::MissingTasklet(id.to_string()))
    }

    fn tasklet_mut(&mut self, id: &str) -> Result<&mut TaskletState, SchedulerError> {
        self.tasklets
            .get_mut(id)
            .ok_or_else(|| SchedulerError::MissingTasklet(id.to_string()))
    }

    fn channel(&self, id: &str) -> Result<&ChannelState, SchedulerError> {
        self.channels
            .get(id)
            .ok_or_else(|| SchedulerError::MissingChannel(id.to_string()))
    }

    fn channel_mut(&mut self, id: &str) -> Result<&mut ChannelState, SchedulerError> {
        self.channels
            .get_mut(id)
            .ok_or_else(|| SchedulerError::MissingChannel(id.to_string()))
    }
}

impl ChannelState {
    fn balance(&self) -> i64 {
        self.blocked_senders.len() as i64 - self.blocked_receivers.len() as i64
    }
}

fn preferred_continuation<'a>(preference: i64, sender: &'a str, receiver: &'a str) -> &'a str {
    if preference >= 1 {
        sender
    } else {
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_channel_blocks_sender_then_matches_receiver_with_rust_owned_ids() {
        let mut scheduler = CoreScheduler::new();
        let sender = scheduler.create_tasklet();
        let receiver = scheduler.create_tasklet();
        let channel = scheduler.create_channel(-1);

        assert_eq!(
            scheduler.send(sender, channel),
            Ok(CoreChannelOperationResult::Blocked {
                tasklet: sender,
                channel,
                direction: CoreChannelDirection::Send,
                balance: 1,
            })
        );
        assert_eq!(
            scheduler.tasklet_lifecycle(sender),
            Ok(CoreTaskletLifecycle::Blocked)
        );
        assert_eq!(
            scheduler.tasklet_blocked_on(sender),
            Ok(Some(CoreBlockedOnChannel {
                channel,
                direction: CoreChannelDirection::Send,
            }))
        );
        assert_eq!(scheduler.queue_front(channel), Ok(Some(sender)));

        assert_eq!(
            scheduler.receive(receiver, channel),
            Ok(CoreChannelOperationResult::Matched {
                sender,
                receiver,
                channel,
                preferred: receiver,
                peer_runs_immediately: false,
                balance: 0,
            })
        );
        assert_eq!(
            scheduler.tasklet_lifecycle(sender),
            Ok(CoreTaskletLifecycle::Runnable)
        );
        assert_eq!(
            scheduler.tasklet_lifecycle(receiver),
            Ok(CoreTaskletLifecycle::Runnable)
        );
        assert_eq!(scheduler.queue_front(channel), Ok(None));
        assert_eq!(
            scheduler
                .channel_snapshot(channel)
                .expect("snapshot")
                .balance,
            0
        );
    }

    #[test]
    fn core_channel_blocks_receiver_then_matches_sender_and_respects_sender_preference() {
        let mut scheduler = CoreScheduler::new();
        let sender = scheduler.create_tasklet();
        let receiver = scheduler.create_tasklet();
        let channel = scheduler.create_channel(99);

        assert_eq!(
            scheduler.receive(receiver, channel),
            Ok(CoreChannelOperationResult::Blocked {
                tasklet: receiver,
                channel,
                direction: CoreChannelDirection::Receive,
                balance: -1,
            })
        );
        assert_eq!(scheduler.queue_front(channel), Ok(Some(receiver)));
        assert_eq!(
            scheduler
                .channel_snapshot(channel)
                .expect("snapshot")
                .preference,
            1
        );

        assert_eq!(
            scheduler.send(sender, channel),
            Ok(CoreChannelOperationResult::Matched {
                sender,
                receiver,
                channel,
                preferred: sender,
                peer_runs_immediately: false,
                balance: 0,
            })
        );
        assert_eq!(scheduler.tasklet_blocked_on(receiver), Ok(None));
    }

    #[test]
    fn core_channel_match_results_report_legacy_peer_handoff_preference() {
        let mut scheduler = CoreScheduler::new();

        let sender = scheduler.create_tasklet();
        let receiver = scheduler.create_tasklet();
        let receiver_preferred_channel = scheduler.create_channel(-1);
        assert!(matches!(
            scheduler.receive(receiver, receiver_preferred_channel),
            Ok(CoreChannelOperationResult::Blocked { .. })
        ));
        assert_eq!(
            scheduler.send(sender, receiver_preferred_channel),
            Ok(CoreChannelOperationResult::Matched {
                sender,
                receiver,
                channel: receiver_preferred_channel,
                preferred: receiver,
                peer_runs_immediately: true,
                balance: 0,
            })
        );

        let sender = scheduler.create_tasklet();
        let receiver = scheduler.create_tasklet();
        let sender_preferred_channel = scheduler.create_channel(1);
        assert!(matches!(
            scheduler.send(sender, sender_preferred_channel),
            Ok(CoreChannelOperationResult::Blocked { .. })
        ));
        assert_eq!(
            scheduler.receive(receiver, sender_preferred_channel),
            Ok(CoreChannelOperationResult::Matched {
                sender,
                receiver,
                channel: sender_preferred_channel,
                preferred: sender,
                peer_runs_immediately: true,
                balance: 0,
            })
        );

        let sender = scheduler.create_tasklet();
        let receiver = scheduler.create_tasklet();
        let neutral_send_channel = scheduler.create_channel(0);
        assert!(matches!(
            scheduler.receive(receiver, neutral_send_channel),
            Ok(CoreChannelOperationResult::Blocked { .. })
        ));
        assert_eq!(
            scheduler.send(sender, neutral_send_channel),
            Ok(CoreChannelOperationResult::Matched {
                sender,
                receiver,
                channel: neutral_send_channel,
                preferred: receiver,
                peer_runs_immediately: false,
                balance: 0,
            })
        );

        let sender = scheduler.create_tasklet();
        let receiver = scheduler.create_tasklet();
        let neutral_receive_channel = scheduler.create_channel(0);
        assert!(matches!(
            scheduler.send(sender, neutral_receive_channel),
            Ok(CoreChannelOperationResult::Blocked { .. })
        ));
        assert_eq!(
            scheduler.receive(receiver, neutral_receive_channel),
            Ok(CoreChannelOperationResult::Matched {
                sender,
                receiver,
                channel: neutral_receive_channel,
                preferred: receiver,
                peer_runs_immediately: false,
                balance: 0,
            })
        );
    }

    #[test]
    fn core_channel_block_trap_rejects_without_mutating_queues() {
        let mut scheduler = CoreScheduler::new();
        let tasklet = scheduler.create_tasklet();
        let channel = scheduler.create_channel(-1);
        scheduler
            .set_tasklet_block_trap(tasklet, true)
            .expect("block trap set");

        assert_eq!(
            scheduler.send(tasklet, channel),
            Err(CoreSchedulerHandleError::BlockingDisallowed {
                tasklet,
                channel,
                direction: CoreChannelDirection::Send,
            })
        );
        assert_eq!(
            scheduler.channel_snapshot(channel).expect("snapshot"),
            CoreChannelSnapshot {
                preference: -1,
                closing: false,
                closed: false,
                balance: 0,
                blocked_senders: Vec::new(),
                blocked_receivers: Vec::new(),
            }
        );
        assert_eq!(
            scheduler.tasklet_lifecycle(tasklet),
            Ok(CoreTaskletLifecycle::Runnable)
        );

        assert_eq!(
            scheduler.receive(tasklet, channel),
            Err(CoreSchedulerHandleError::BlockingDisallowed {
                tasklet,
                channel,
                direction: CoreChannelDirection::Receive,
            })
        );
        assert_eq!(
            scheduler
                .channel_snapshot(channel)
                .expect("snapshot")
                .balance,
            0
        );
    }

    #[test]
    fn core_channel_close_open_clear_and_remove_update_rust_owned_state() {
        let mut scheduler = CoreScheduler::new();
        let sender = scheduler.create_tasklet();
        let receiver = scheduler.create_tasklet();
        let channel = scheduler.create_channel(0);

        scheduler.send(sender, channel).expect("sender blocks");
        scheduler.close_channel(channel).expect("close succeeds");
        let closing = scheduler.channel_snapshot(channel).expect("snapshot");
        assert!(closing.closing);
        assert!(!closing.closed);
        assert_eq!(closing.balance, 1);

        scheduler
            .remove_tasklet_from_channel(sender)
            .expect("blocked sender removed");
        let closed = scheduler.channel_snapshot(channel).expect("snapshot");
        assert!(closed.closing);
        assert!(closed.closed);
        assert_eq!(closed.balance, 0);

        scheduler.open_channel(channel).expect("open succeeds");
        assert!(
            !scheduler
                .channel_snapshot(channel)
                .expect("snapshot")
                .closed
        );
        scheduler
            .receive(receiver, channel)
            .expect("receiver blocks after open");
        assert_eq!(
            scheduler.clear_channel(channel).expect("clear succeeds"),
            vec![receiver]
        );
        let cleared = scheduler.channel_snapshot(channel).expect("snapshot");
        assert_eq!(cleared.balance, 0);
        assert!(cleared.blocked_receivers.is_empty());
        assert_eq!(
            scheduler.tasklet_lifecycle(receiver),
            Ok(CoreTaskletLifecycle::Complete)
        );
    }

    #[test]
    fn core_tasklet_runtime_snapshot_tracks_bridge_lifecycle_flags() {
        let mut scheduler = CoreScheduler::new();
        let tasklet = scheduler.create_tasklet();

        assert_eq!(
            scheduler.tasklet_snapshot(tasklet).expect("snapshot"),
            CoreTaskletSnapshot {
                lifecycle: CoreTaskletLifecycle::Runnable,
                blocked_on: None,
                block_trap: false,
                alive: true,
                scheduled: false,
                paused: false,
                times_switched_to: 0,
            }
        );

        scheduler
            .update_tasklet_runtime_state(tasklet, true, false, 0)
            .expect("runtime state updates without scheduling");
        assert!(
            !scheduler
                .tasklet_snapshot(tasklet)
                .expect("snapshot")
                .scheduled
        );
        let queue = scheduler.create_run_queue();
        scheduler
            .schedule_tasklet_back(queue, tasklet)
            .expect("run queue owns scheduled state");
        scheduler
            .update_tasklet_runtime_state(tasklet, true, false, 0)
            .expect("runtime sync preserves queue-owned scheduling");
        assert_eq!(
            scheduler.tasklet_snapshot(tasklet).expect("snapshot"),
            CoreTaskletSnapshot {
                lifecycle: CoreTaskletLifecycle::Runnable,
                blocked_on: None,
                block_trap: false,
                alive: true,
                scheduled: true,
                paused: false,
                times_switched_to: 0,
            }
        );

        scheduler
            .update_tasklet_runtime_state(tasklet, false, false, 1)
            .expect("complete state updates");
        assert_eq!(
            scheduler.tasklet_snapshot(tasklet).expect("snapshot"),
            CoreTaskletSnapshot {
                lifecycle: CoreTaskletLifecycle::Complete,
                blocked_on: None,
                block_trap: false,
                alive: false,
                scheduled: false,
                paused: false,
                times_switched_to: 1,
            }
        );
    }

    #[test]
    fn core_pause_resume_owns_paused_lifecycle_and_queue_removal() {
        let mut scheduler = CoreScheduler::new();
        let queue = scheduler.create_run_queue();
        let tasklet = scheduler.create_tasklet();

        scheduler
            .schedule_tasklet_back(queue, tasklet)
            .expect("tasklet scheduled");
        scheduler
            .pause_tasklet(tasklet)
            .expect("tasklet paused by core");

        assert_eq!(scheduler.runnable_tasklet_count(queue), Ok(0));
        assert_eq!(
            scheduler
                .tasklet_snapshot(tasklet)
                .expect("paused snapshot"),
            CoreTaskletSnapshot {
                lifecycle: CoreTaskletLifecycle::Runnable,
                blocked_on: None,
                block_trap: false,
                alive: true,
                scheduled: false,
                paused: true,
                times_switched_to: 0,
            }
        );

        scheduler
            .resume_tasklet(tasklet)
            .expect("tasklet resumed by core");
        assert_eq!(
            scheduler
                .tasklet_snapshot(tasklet)
                .expect("resumed snapshot"),
            CoreTaskletSnapshot {
                lifecycle: CoreTaskletLifecycle::Runnable,
                blocked_on: None,
                block_trap: false,
                alive: true,
                scheduled: false,
                paused: false,
                times_switched_to: 0,
            }
        );

        scheduler
            .pause_tasklet(tasklet)
            .expect("tasklet paused again");
        scheduler
            .schedule_tasklet_back(queue, tasklet)
            .expect("scheduling resumes tasklet");
        let snapshot = scheduler
            .tasklet_snapshot(tasklet)
            .expect("scheduled snapshot");
        assert!(snapshot.alive);
        assert!(snapshot.scheduled);
        assert!(!snapshot.paused);
        assert_eq!(scheduler.runnable_tasklet_count(queue), Ok(1));
    }

    #[test]
    fn core_run_queue_dedupes_counts_and_pops_fifo() {
        let mut scheduler = CoreScheduler::new();
        let queue = scheduler.create_run_queue();
        let first = scheduler.create_tasklet();
        let second = scheduler.create_tasklet();

        scheduler
            .schedule_tasklet_back(queue, first)
            .expect("first queued");
        scheduler
            .schedule_tasklet_back(queue, second)
            .expect("second queued");
        scheduler
            .schedule_tasklet_back(queue, first)
            .expect("first requeued without duplication");

        assert_eq!(scheduler.runnable_tasklet_count(queue), Ok(2));
        assert_eq!(scheduler.pop_next_runnable_tasklet(queue), Ok(Some(second)));
        assert_eq!(scheduler.pop_next_runnable_tasklet(queue), Ok(Some(first)));
        assert_eq!(scheduler.pop_next_runnable_tasklet(queue), Ok(None));
        assert_eq!(scheduler.runnable_tasklet_count(queue), Ok(0));
        assert!(
            !scheduler
                .tasklet_snapshot(first)
                .expect("snapshot")
                .scheduled
        );
    }

    #[test]
    fn core_run_queue_remove_and_clear_update_snapshots() {
        let mut scheduler = CoreScheduler::new();
        let queue = scheduler.create_run_queue();
        let first = scheduler.create_tasklet();
        let second = scheduler.create_tasklet();

        scheduler
            .schedule_tasklet_back(queue, first)
            .expect("first queued");
        scheduler
            .schedule_tasklet_back(queue, second)
            .expect("second queued");
        scheduler
            .remove_runnable_tasklet(first)
            .expect("first removed");

        assert_eq!(scheduler.runnable_tasklet_count(queue), Ok(1));
        assert!(
            !scheduler
                .tasklet_snapshot(first)
                .expect("first snapshot")
                .scheduled
        );
        assert!(
            scheduler
                .tasklet_snapshot(second)
                .expect("second snapshot")
                .scheduled
        );

        assert_eq!(scheduler.clear_run_queue(queue), Ok(vec![second]));
        assert_eq!(scheduler.runnable_tasklet_count(queue), Ok(0));
        assert!(
            !scheduler
                .tasklet_snapshot(second)
                .expect("second snapshot")
                .scheduled
        );
    }

    #[test]
    fn core_run_queue_is_per_owner_queue() {
        let mut scheduler = CoreScheduler::new();
        let first_queue = scheduler.create_run_queue();
        let second_queue = scheduler.create_run_queue();
        let first = scheduler.create_tasklet();
        let second = scheduler.create_tasklet();

        scheduler
            .schedule_tasklet_back(first_queue, first)
            .expect("first queued");
        scheduler
            .schedule_tasklet_back(second_queue, second)
            .expect("second queued");

        assert_eq!(scheduler.runnable_tasklet_count(first_queue), Ok(1));
        assert_eq!(scheduler.runnable_tasklet_count(second_queue), Ok(1));
        assert_eq!(
            scheduler.pop_next_runnable_tasklet(first_queue),
            Ok(Some(first))
        );
        assert_eq!(scheduler.pop_next_runnable_tasklet(first_queue), Ok(None));
        assert_eq!(
            scheduler.pop_next_runnable_tasklet(second_queue),
            Ok(Some(second))
        );
    }

    #[test]
    fn simple_run_order_completes_tasklets() {
        let scenario = Scenario {
            nested_tasklets: true,
            channel_callbacks: false,
            channels: Vec::new(),
            tasklets: vec![
                TaskletSpec {
                    id: String::from("t1"),
                    initially_scheduled: true,
                    initially_bound: true,
                    body: vec![Operation::Append {
                        target: String::from("completed"),
                        value: json!("t1"),
                    }],
                },
                TaskletSpec {
                    id: String::from("t2"),
                    initially_scheduled: true,
                    initially_bound: true,
                    body: vec![Operation::Append {
                        target: String::from("completed"),
                        value: json!("t2"),
                    }],
                },
            ],
            entrypoint: Entrypoint::RunScheduler,
        };

        let trace = run_scenario(&scenario).expect("scenario runs");
        assert_eq!(trace.final_state["run_count"], json!(1));
        assert_eq!(trace.final_state["completed"], json!(["t1", "t2"]));
        assert_eq!(trace.final_state["tasklets"]["t1"]["alive"], json!(false));
    }
}
