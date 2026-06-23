//! Python/C API compatibility bridge for the Rust scheduler migration.
//!
//! Scheduler state and parity fixtures should live in Rust crates. This crate
//! keeps the legacy `_scheduler`/`scheduler._C_API` surface available so the
//! unchanged Python tests and existing C/Python consumers can verify behavior
//! while the implementation is moved behind the Rust boundary.

#[cfg(test)]
use carbon_scheduler_core::CoreTaskletLifecycle;
use carbon_scheduler_core::{
    CoreChannelDirection, CoreChannelId, CoreChannelOperationResult, CoreChannelSnapshot,
    CorePayloadToken, CoreRunQueueId, CoreScheduler, CoreSchedulerHandleError, CoreTaskletId,
    CoreTaskletSnapshot,
};
use carbon_scheduler_ffi::{carbon_scheduler_abi_version, CarbonSchedulerStatus};
use pyo3::exceptions::{
    PyBaseException, PyException, PyRuntimeError, PyStopIteration, PySystemExit, PyTypeError,
    PyValueError,
};
use pyo3::ffi;
use pyo3::gc::PyVisit;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyLong, PyString, PyTuple, PyType};
use pyo3::AsPyPointer;
use pyo3::PyTraverseError;
use std::cell::{RefCell, UnsafeCell};
use std::collections::{HashSet, VecDeque};
use std::ffi::{c_char, c_void};
use std::mem::{forget, ManuallyDrop};
use std::ops::Deref;
use std::os::raw::c_long;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pyo3::create_exception!(_scheduler, TaskletExit, PySystemExit);
pyo3::create_exception!(carbon_scheduler_python, TaskletBlocked, PyBaseException);
pyo3::create_exception!(carbon_scheduler_python, TaskletPaused, PyBaseException);

pub const PYTHON_BRIDGE_STATUS: &str = "pyo3_smoke";

const REQUIRED_EXTENSION_MODULE_NAMES: &[&str] = &[
    "_scheduler",
    "_scheduler_debug",
    "_scheduler_trinitydev",
    "_scheduler_internal",
];

const INITIAL_PUBLIC_SYMBOLS: &[&str] = &[
    "TaskletExit",
    "_C_API",
    "callable_wrapper",
    "tasklet",
    "channel",
    "schedule_manager",
    "abi_version",
    "bridge_status",
    "core_status",
    "getcurrent",
    "getmain",
    "getruncount",
    "calculateruncount",
    "run",
    "run_n_tasklets",
    "schedule",
    "schedule_remove",
    "get_schedule_manager",
    "get_number_of_active_schedule_managers",
    "get_number_of_active_channels",
    "unblock_all_channels",
    "get_all_time_tasklet_count",
    "get_active_tasklet_count",
    "set_use_nested_tasklets",
    "get_use_nested_tasklets",
    "enable_softswitch",
    "enable_soft_switch",
    "set_channel_callback",
    "get_channel_callback",
    "set_schedule_callback",
    "get_schedule_callback",
    "get_thread_info",
    "switch_trap",
];

#[cfg(test)]
const LEGACY_SCHEDULER_INIT: &str =
    include_str!("../../../carbonengine/scheduler/python/scheduler/__init__.py");

#[cfg(test)]
const SCHEDULER_CAPSULE_NAME: &str = carbon_scheduler_ffi::SCHEDULER_C_API_CAPSULE_NAME;
const SCHEDULER_CAPSULE_C_NAME: &[u8] = carbon_scheduler_ffi::SCHEDULER_C_API_CAPSULE_NAME_CSTR;
const THREAD_CLEANUP_SENTINEL_KEY: &str = "_carbon_scheduler_thread_cleanup";
const TASKLET_CONTEXT_DEFAULT: &str = "rust-pyo3-smoke";
const TASKLET_CONTEXT_CALLABLE_BOUND: &str = "rust-pyo3-smoke:callable-bound";

static USE_NESTED_TASKLETS: AtomicBool = AtomicBool::new(true);
static ACTIVE_SCHEDULE_MANAGERS: AtomicU32 = AtomicU32::new(0);
static ACTIVE_CHANNELS: AtomicU32 = AtomicU32::new(0);
static ALL_TIME_TASKLETS: AtomicU32 = AtomicU32::new(1);
static ACTIVE_TASKLETS: AtomicU32 = AtomicU32::new(1);
static ACTIVE_CHANNEL_OBJECTS: Mutex<Vec<usize>> = Mutex::new(Vec::new());
static THREAD_RUN_QUEUES: Mutex<Vec<ThreadRunQueue>> = Mutex::new(Vec::new());
static PENDING_RUN_QUEUE_REMOVALS: Mutex<Vec<PendingRunQueueRemoval>> = Mutex::new(Vec::new());
static SCHEDULE_CALLBACK: Mutex<Option<PyObject>> = Mutex::new(None);
static SCHEDULE_FAST_CALLBACK: Mutex<Option<ScheduleHookFunc>> = Mutex::new(None);
static SCHEDULE_CALLBACK_PRESENT: AtomicBool = AtomicBool::new(false);
static SCHEDULE_FAST_CALLBACK_PRESENT: AtomicBool = AtomicBool::new(false);
static LAST_TIMEOUT_COMPLETED_TASKLETS: AtomicI32 = AtomicI32::new(0);
static LAST_TIMEOUT_SWITCHED_TASKLETS: AtomicI32 = AtomicI32::new(0);
static RUN_QUEUE_GENERATION: AtomicU64 = AtomicU64::new(0);
static FOREIGN_RUN_QUEUE_GENERATION: AtomicU64 = AtomicU64::new(0);
static BRIDGE_CORE_SCHEDULER: OnceLock<Mutex<CoreScheduler>> = OnceLock::new();
const MIN_POSITIVE_RUN_TIMEOUT_NANOS: u128 = 10_000_000;

type ScheduleHookFunc = extern "C" fn(*mut ffi::PyObject, *mut ffi::PyObject) -> i32;

struct ThreadRunQueue {
    thread_id: ThreadId,
    core_queue_id: CoreRunQueueId,
    queue: VecDeque<QueuedTasklet>,
    queued_ids: HashSet<CoreTaskletId>,
}

struct QueuedTasklet {
    core_id: CoreTaskletId,
    object: PyObject,
}

#[derive(Clone, Copy)]
struct PendingRunQueueRemoval {
    thread_id: ThreadId,
    core_id: CoreTaskletId,
}

trait GilDropValue: Default {
    fn abandon_without_python(self);
}

impl GilDropValue for Option<PyObject> {
    fn abandon_without_python(self) {
        if let Some(object) = self {
            forget(object);
        }
    }
}

impl GilDropValue for Option<ThreadRunQueue> {
    fn abandon_without_python(self) {
        if let Some(mut queue) = self {
            while let Some(tasklet) = queue.queue.pop_front() {
                forget(tasklet.object);
            }
        }
    }
}

struct GilDropRefCell<T: GilDropValue>(ManuallyDrop<RefCell<T>>);

impl<T: GilDropValue> GilDropRefCell<T> {
    const fn new(value: T) -> Self {
        Self(ManuallyDrop::new(RefCell::new(value)))
    }
}

impl<T: GilDropValue> Deref for GilDropRefCell<T> {
    type Target = RefCell<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: GilDropValue> Drop for GilDropRefCell<T> {
    fn drop(&mut self) {
        if unsafe { ffi::Py_IsInitialized() } == 0
            || unsafe { ffi::PyGILState_GetThisThreadState() }.is_null()
        {
            let cell = unsafe { ManuallyDrop::take(&mut self.0) };
            cell.into_inner().abandon_without_python();
            return;
        }
        Python::with_gil(|_| {
            *self.0.borrow_mut() = T::default();
        });
        unsafe {
            ManuallyDrop::drop(&mut self.0);
        }
    }
}

struct ThreadScheduleManager {
    manager: Py<ScheduleManager>,
}

impl ThreadScheduleManager {
    fn new(py: Python<'_>) -> PyResult<Self> {
        let manager = Py::new(py, ScheduleManager)?;
        ACTIVE_SCHEDULE_MANAGERS.fetch_add(1, Ordering::SeqCst);
        Ok(Self { manager })
    }

    fn abandon_without_python(self) {
        ACTIVE_SCHEDULE_MANAGERS.fetch_sub(1, Ordering::SeqCst);
        let this = ManuallyDrop::new(self);
        unsafe {
            forget(ptr::read(&this.manager));
        }
    }
}

impl Drop for ThreadScheduleManager {
    fn drop(&mut self) {
        ACTIVE_SCHEDULE_MANAGERS.fetch_sub(1, Ordering::SeqCst);
    }
}

impl GilDropValue for Option<ThreadScheduleManager> {
    fn abandon_without_python(self) {
        if let Some(manager) = self {
            manager.abandon_without_python();
        }
    }
}

struct ThreadCleanupGuard {
    thread_id: ThreadId,
}

impl ThreadCleanupGuard {
    fn new() -> Self {
        Self {
            thread_id: std::thread::current().id(),
        }
    }
}

impl Drop for ThreadCleanupGuard {
    fn drop(&mut self) {
        if unsafe { ffi::Py_IsInitialized() } == 0 {
            return;
        }
        unsafe {
            let gil_state = ffi::PyGILState_Ensure();
            let py = Python::assume_gil_acquired();
            cleanup_thread_state_without_tls(py, self.thread_id);
            ffi::PyGILState_Release(gil_state);
        }
    }
}

struct GreenletThreadCache {
    greenlet_type: PyObject,
    getcurrent: PyObject,
    greenlet_exit: PyObject,
}

impl GreenletThreadCache {
    fn new(py: Python<'_>) -> PyResult<Self> {
        let module = PyModule::import_bound(py, "greenlet").map_err(|error| {
            PyRuntimeError::new_err(format!(
                "greenlet is required for scheduler continuation support: {error}"
            ))
        })?;
        Ok(Self {
            greenlet_type: module.getattr("greenlet")?.to_object(py),
            getcurrent: module.getattr("getcurrent")?.to_object(py),
            greenlet_exit: module.getattr("GreenletExit")?.to_object(py),
        })
    }

    fn abandon_without_python(self) {
        forget(self.greenlet_type);
        forget(self.getcurrent);
        forget(self.greenlet_exit);
    }
}

impl GilDropValue for Option<GreenletThreadCache> {
    fn abandon_without_python(self) {
        if let Some(cache) = self {
            cache.abandon_without_python();
        }
    }
}

thread_local! {
    static THREAD_CLEANUP_GUARD: ThreadCleanupGuard = ThreadCleanupGuard::new();
    static CURRENT_TASKLET: RefCell<Option<*mut ffi::PyObject>> = const { RefCell::new(None) };
    static SCHEDULE_MANAGER: GilDropRefCell<Option<ThreadScheduleManager>> = const { GilDropRefCell::new(None) };
    static EXECUTING_TASKLET: GilDropRefCell<Option<PyObject>> = const { GilDropRefCell::new(None) };
    static CHANNEL_CALLBACK: GilDropRefCell<Option<PyObject>> = const { GilDropRefCell::new(None) };
    static CHANNEL_CALLBACK_TOUCHED: RefCell<bool> = const { RefCell::new(false) };
    static GREENLET_CACHE: GilDropRefCell<Option<GreenletThreadCache>> = const { GilDropRefCell::new(None) };
    static CURRENT_THREAD_RUN_QUEUE: GilDropRefCell<Option<ThreadRunQueue>> = const { GilDropRefCell::new(None) };
    static FOREIGN_RUN_QUEUE_SEEN: RefCell<u64> = const { RefCell::new(0) };
    static RUN_QUEUE_COUNT_CACHE: RefCell<Option<(u64, usize)>> = const { RefCell::new(None) };
    static TASKLET_CONTEXT_C_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn cleanup_thread_state(py: Python<'_>, thread_id: ThreadId) {
    if let Ok(tasklets) = take_thread_run_queue(py, thread_id) {
        for tasklet in tasklets {
            finish_tasklet_for_thread_exit(py, &tasklet);
        }
    }
    cleanup_thread_channels(py, thread_id);
}

fn cleanup_thread_state_without_tls(py: Python<'_>, thread_id: ThreadId) {
    if let Ok(tasklets) = take_global_thread_run_queue(thread_id) {
        for tasklet in tasklets {
            finish_tasklet_for_thread_exit(py, &tasklet);
        }
    }
    cleanup_thread_channels(py, thread_id);
}

#[pyclass(name = "_ThreadCleanupSentinel", module = "_scheduler")]
struct ThreadCleanupSentinel {
    thread_id: ThreadId,
}

impl ThreadCleanupSentinel {
    fn new() -> Self {
        Self {
            thread_id: current_thread_id(),
        }
    }
}

impl Drop for ThreadCleanupSentinel {
    fn drop(&mut self) {
        if unsafe { ffi::Py_IsInitialized() } == 0 {
            return;
        }
        Python::with_gil(|py| cleanup_thread_state(py, self.thread_id));
    }
}

#[pyclass(name = "CallableWrapper", module = "scheduler", subclass, weakref)]
struct CallableWrapper {
    callable: PyObject,
}

#[pymethods]
impl CallableWrapper {
    #[new]
    fn new(py: Python<'_>, callable: PyObject) -> PyResult<Self> {
        if !callable.bind(py).is_callable() {
            return Err(PyTypeError::new_err(
                "CallableWrapper only accepts a callable as an argument",
            ));
        }

        Ok(Self { callable })
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        Ok(self.callable.bind(py).call(args, kwargs)?.to_object(py))
    }
}

#[pyclass(name = "_TaskletGreenletRunner", module = "_scheduler")]
struct TaskletGreenletRunner {
    tasklet: Option<PyObject>,
}

#[pymethods]
impl TaskletGreenletRunner {
    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        let tasklet_object = self
            .tasklet
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("tasklet has been unbound"))?
            .clone_ref(py);
        let (callable, dont_raise, context_manager_getter, exception_handler, context) = {
            let tasklet = tasklet_object
                .bind(py)
                .downcast::<Tasklet>()
                .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
            let tasklet = tasklet.try_borrow()?;
            (
                tasklet
                    .callable
                    .as_ref()
                    .ok_or_else(|| PyRuntimeError::new_err("tasklet has no callable"))?
                    .clone_ref(py),
                tasklet.dont_raise,
                tasklet
                    .context_manager_getter
                    .as_ref()
                    .map(|getter| getter.clone_ref(py)),
                tasklet
                    .exception_handler
                    .as_ref()
                    .map(|handler| handler.clone_ref(py)),
                tasklet.context.clone(),
            )
        };

        let mut exit_callable = None;
        let _context_manager = if let Some(context_manager_getter) = context_manager_getter.as_ref()
        {
            let context_manager =
                context_manager_getter.call1(py, (tasklet_object.clone_ref(py),))?;
            let enter_callable = context_manager.getattr(py, "__enter__")?;
            let context_exit = context_manager.getattr(py, "__exit__")?;
            enter_callable.call0(py)?;
            exit_callable = Some(context_exit);
            Some(context_manager)
        } else {
            None
        };

        let kwargs_ptr = kwargs.map_or(ptr::null_mut(), pyo3::AsPyPointer::as_ptr);
        let result = unsafe { ffi::PyObject_Call(callable.as_ptr(), args.as_ptr(), kwargs_ptr) };
        if result.is_null() {
            let error = PyErr::fetch(py);
            if dont_raise && !error.is_instance_of::<TaskletExit>(py) {
                if let Some(handler) = exception_handler.as_ref() {
                    call_exception_handler(py, handler, &error, &context);
                }
                if let Some(exit_callable) = exit_callable.as_ref() {
                    call_context_exit(py, exit_callable)?;
                }
                return Ok(py.None());
            }
            return Err(error);
        }

        let result = unsafe { PyObject::from_owned_ptr(py, result) };
        if let Some(exit_callable) = exit_callable.as_ref() {
            call_context_exit(py, exit_callable)?;
        }
        Ok(result)
    }
}

#[pyfunction]
pub fn bridge_status() -> &'static str {
    PYTHON_BRIDGE_STATUS
}

pub fn required_extension_module_names() -> &'static [&'static str] {
    REQUIRED_EXTENSION_MODULE_NAMES
}

pub fn initial_public_symbols() -> &'static [&'static str] {
    INITIAL_PUBLIC_SYMBOLS
}

#[pyclass(name = "tasklet", module = "_scheduler", subclass, weakref)]
pub struct Tasklet {
    core_id: CoreTaskletId,
    callable: Option<PyObject>,
    args: Option<PyObject>,
    kwargs: Option<PyObject>,
    last_args: Option<PyObject>,
    last_kwargs: Option<PyObject>,
    greenlet: Option<PyObject>,
    pending_exception: Option<TaskletException>,
    blocked_channel: Option<PyObject>,
    blocked_direction: Option<ChannelBlockDirection>,
    context_manager_getter: Option<PyObject>,
    exception_handler: Option<PyObject>,
    callable_bound: bool,
    is_main: bool,
    alive: bool,
    blocked: bool,
    scheduled: bool,
    paused: bool,
    continuation_pending: bool,
    kill_pending: bool,
    block_trap: bool,
    times_switched_to: u64,
    method_name: String,
    module_name: String,
    context: String,
    file_name: String,
    line_number: i64,
    parent_callsite: String,
    start_time: i64,
    end_time: i64,
    run_time: f64,
    highlighted: bool,
    dont_raise: bool,
    counted_active: bool,
    skip_next_channel_callback: bool,
    owner_thread: ThreadId,
}

impl Tasklet {
    fn unbound(is_main: bool) -> Self {
        let counted_active = !is_main;
        if counted_active {
            ALL_TIME_TASKLETS.fetch_add(1, Ordering::SeqCst);
            ACTIVE_TASKLETS.fetch_add(1, Ordering::SeqCst);
        }

        let tasklet = Self {
            core_id: bridge_core_create_tasklet(),
            callable: None,
            args: None,
            kwargs: None,
            last_args: None,
            last_kwargs: None,
            greenlet: None,
            pending_exception: None,
            blocked_channel: None,
            blocked_direction: None,
            context_manager_getter: None,
            exception_handler: None,
            callable_bound: false,
            is_main,
            alive: is_main,
            blocked: false,
            scheduled: false,
            paused: false,
            continuation_pending: false,
            kill_pending: false,
            block_trap: false,
            times_switched_to: u64::from(is_main),
            method_name: String::from("unknown_method"),
            module_name: String::from("unknown_module"),
            context: String::from(TASKLET_CONTEXT_DEFAULT),
            file_name: String::from("unknown_file"),
            line_number: 0,
            parent_callsite: String::new(),
            start_time: 0,
            end_time: 0,
            run_time: 0.0,
            highlighted: false,
            dont_raise: false,
            counted_active,
            skip_next_channel_callback: false,
            owner_thread: std::thread::current().id(),
        };
        tasklet.sync_core_state();
        tasklet
    }

    fn sync_core_state(&self) {
        bridge_core_sync_tasklet_state(
            self.core_id,
            self.alive,
            self.paused,
            self.times_switched_to,
            self.block_trap,
        );
    }

    fn core_snapshot(&self) -> PyResult<CoreTaskletSnapshot> {
        bridge_core_tasklet_snapshot(self.core_id)
    }

    fn bind_callable(&mut self, py: Python<'_>, callable: PyObject) -> PyResult<()> {
        if !callable.bind(py).is_callable() {
            return Err(PyTypeError::new_err("parameter must be callable"));
        }
        self.set_callsite_data(py, &callable)?;
        self.callable = Some(callable);
        self.last_args = None;
        self.last_kwargs = None;
        if let Some(greenlet) = self.greenlet.take() {
            dispose_tasklet_greenlet(py, &greenlet);
        }
        self.callable_bound = true;
        if self.context == TASKLET_CONTEXT_DEFAULT {
            self.context = String::from(TASKLET_CONTEXT_CALLABLE_BOUND);
        }
        self.times_switched_to = 0;
        self.continuation_pending = false;
        self.kill_pending = false;
        self.pending_exception = None;
        self.sync_core_state();
        Ok(())
    }

    fn belongs_to_current_thread(&self) -> bool {
        self.owner_thread == std::thread::current().id()
    }

    fn set_callsite_data(&mut self, py: Python<'_>, callable: &PyObject) -> PyResult<()> {
        self.method_name = String::from("unknown_method");
        self.module_name = String::from("unknown_module");
        self.file_name = String::from("unknown_file");
        self.line_number = 0;

        let callable = callable.bind(py);
        if callable.hasattr("__name__")? {
            self.method_name = callable.getattr("__name__")?.str()?.extract()?;
        }
        if callable.hasattr("__module__")? {
            self.module_name = callable.getattr("__module__")?.str()?.extract()?;
        }
        if callable.hasattr("__code__")? {
            let code = callable.getattr("__code__")?;
            if code.hasattr("co_filename")? {
                self.file_name = code.getattr("co_filename")?.extract()?;
            }
            if code.hasattr("co_firstlineno")? {
                self.line_number = code.getattr("co_firstlineno")?.extract()?;
            }
        }

        Ok(())
    }
}

enum TaskletException {
    Set {
        exc: PyObject,
        value: PyObject,
    },
    Restore {
        exc: PyObject,
        value: PyObject,
        traceback: PyObject,
    },
}

impl TaskletException {
    fn set(exc: PyObject, value: PyObject) -> Self {
        Self::Set { exc, value }
    }

    fn restore(exc: PyObject, value: PyObject, traceback: PyObject) -> Self {
        Self::Restore {
            exc,
            value,
            traceback,
        }
    }

    fn is_tasklet_exit(&self, py: Python<'_>) -> bool {
        let tasklet_exit = py.get_type_bound::<TaskletExit>();
        let exc = match self {
            Self::Set { exc, .. } | Self::Restore { exc, .. } => exc,
        };
        unsafe { ffi::PyErr_GivenExceptionMatches(exc.as_ptr(), tasklet_exit.as_ptr()) != 0 }
    }

    fn raise(self, py: Python<'_>) -> PyErr {
        match self {
            Self::Set { exc, value } => raise_python_exception(py, exc, value, None),
            Self::Restore {
                exc,
                value,
                traceback,
            } => raise_python_exception(py, exc, value, Some(traceback)),
        }
    }

    fn throw_into_greenlet(self, py: Python<'_>, greenlet: &PyObject) -> PyResult<PyObject> {
        let throw = greenlet.bind(py).getattr("throw")?;
        let result = match self {
            Self::Set { exc, value } => {
                if value.bind(py).is_none() {
                    throw.call1((exc,))?
                } else {
                    throw.call1((exc, value))?
                }
            }
            Self::Restore {
                exc,
                value,
                traceback,
            } => throw.call1((exc, value, traceback))?,
        };
        Ok(result.to_object(py))
    }

    fn into_channel_message(self) -> ChannelMessage {
        match self {
            Self::Set { exc, value } => ChannelMessage::SetException { exc, value },
            Self::Restore {
                exc,
                value,
                traceback,
            } => ChannelMessage::RestoreException {
                exc,
                value,
                traceback,
            },
        }
    }

    fn traverse(&self, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
        match self {
            Self::Set { exc, value } => {
                visit.call(exc)?;
                visit.call(value)?;
            }
            Self::Restore {
                exc,
                value,
                traceback,
            } => {
                visit.call(exc)?;
                visit.call(value)?;
                visit.call(traceback)?;
            }
        }
        Ok(())
    }
}

impl Drop for Tasklet {
    fn drop(&mut self) {
        if self.counted_active {
            ACTIVE_TASKLETS.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[pymethods]
impl Tasklet {
    #[new]
    #[pyo3(signature = (callable=None, is_main=false))]
    fn new(py: Python<'_>, callable: Option<PyObject>, is_main: bool) -> PyResult<Self> {
        ensure_thread_cleanup(py)?;
        let mut tasklet = Self::unbound(is_main);
        if let Some(callable) = callable {
            tasklet.bind_callable(py, callable)?;
        }
        Ok(tasklet)
    }

    #[getter]
    fn alive(&self) -> PyResult<bool> {
        Ok(self.core_snapshot()?.alive)
    }

    #[getter]
    fn blocked(&self) -> PyResult<bool> {
        Ok(self.core_snapshot()?.blocked_on.is_some())
    }

    #[getter]
    fn scheduled(&self) -> PyResult<bool> {
        Ok(self.core_snapshot()?.scheduled)
    }

    #[getter]
    fn paused(&self) -> PyResult<bool> {
        Ok(self.core_snapshot()?.paused)
    }

    #[getter]
    fn block_trap(&self) -> PyResult<bool> {
        Ok(self.core_snapshot()?.block_trap)
    }

    #[setter]
    fn set_block_trap(&mut self, value: bool) {
        self.block_trap = value;
        self.sync_core_state();
    }

    #[getter]
    fn is_current(&self) -> bool {
        self.is_main
    }

    #[getter]
    fn is_main(&self) -> bool {
        self.is_main
    }

    #[getter]
    fn thread_id(&self) -> u64 {
        0
    }

    #[getter]
    fn next(&self, py: Python<'_>) -> PyObject {
        py.None()
    }

    #[getter]
    fn previous(&self, py: Python<'_>) -> PyObject {
        py.None()
    }

    #[getter]
    fn parent(&self, py: Python<'_>) -> PyObject {
        py.None()
    }

    #[getter]
    fn times_switched_to(&self) -> PyResult<u64> {
        Ok(self.core_snapshot()?.times_switched_to)
    }

    #[getter]
    fn context(&self) -> &str {
        self.context.as_str()
    }

    #[setter]
    fn set_context(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.context = legacy_std_string_from_py_object(value)?;
        Ok(())
    }

    #[getter]
    fn frame(&self, _py: Python<'_>) -> PyResult<PyObject> {
        Err(PyRuntimeError::new_err("frame Not implemented"))
    }

    #[getter]
    fn method_name(&self) -> &str {
        self.method_name.as_str()
    }

    #[getter]
    fn module_name(&self) -> &str {
        self.module_name.as_str()
    }

    #[getter]
    fn file_name(&self) -> &str {
        self.file_name.as_str()
    }

    #[getter]
    fn line_number(&self) -> i64 {
        self.line_number
    }

    #[getter]
    fn parent_callsite(&self) -> &str {
        self.parent_callsite.as_str()
    }

    #[setter]
    fn set_parent_callsite(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.parent_callsite = legacy_std_string_from_py_object(value)?;
        Ok(())
    }

    #[getter(startTime)]
    fn start_time(&self) -> i64 {
        self.start_time
    }

    #[getter(endTime)]
    fn end_time(&self) -> i64 {
        self.end_time
    }

    #[getter(runTime)]
    fn run_time(&self) -> f64 {
        self.run_time
    }

    #[setter(runTime)]
    fn set_run_time(&mut self, value: f64) {
        self.run_time = value;
    }

    #[getter]
    fn highlighted(&self) -> bool {
        self.highlighted
    }

    #[setter]
    fn set_highlighted(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        if !value.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(
                "highlighted must be either a True or False value",
            ));
        }
        self.highlighted = value.is_truthy()?;
        Ok(())
    }

    #[getter]
    fn dont_raise(&self) -> bool {
        self.dont_raise
    }

    #[setter]
    fn set_dont_raise(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        if !value.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(
                "dont_raise must be either a True or False value",
            ));
        }
        if self.args.is_some() || self.kwargs.is_some() || self.scheduled {
            return Err(PyRuntimeError::new_err(
                "dont_raise cannot be altered after the Tasklet has been bound",
            ));
        }
        self.dont_raise = value.is_truthy()?;
        Ok(())
    }

    #[getter]
    fn context_manager_getter(&self, py: Python<'_>) -> PyObject {
        self.context_manager_getter
            .as_ref()
            .map(|getter| getter.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    #[setter]
    fn set_context_manager_getter(
        &mut self,
        py: Python<'_>,
        value: Option<PyObject>,
    ) -> PyResult<()> {
        if let Some(value) = value {
            if !value.bind(py).is_callable() {
                return Err(PyTypeError::new_err(
                    "context_manager_fun must be a callable that returns a context manager object",
                ));
            }
            self.context_manager_getter = Some(value);
        } else {
            self.context_manager_getter = None;
        }
        Ok(())
    }

    #[getter]
    fn exception_handler(&self, py: Python<'_>) -> PyObject {
        self.exception_handler
            .as_ref()
            .map(|handler| handler.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    #[setter]
    fn set_exception_handler(&mut self, py: Python<'_>, value: Option<PyObject>) -> PyResult<()> {
        if let Some(value) = value {
            if !value.bind(py).is_callable() {
                return Err(PyTypeError::new_err("exception_handler must be a callable"));
            }
            self.exception_handler = Some(value);
        } else {
            self.exception_handler = None;
        }
        Ok(())
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Some(callable) = &self.callable {
            visit.call(callable)?;
        }
        if let Some(args) = &self.args {
            visit.call(args)?;
        }
        if let Some(kwargs) = &self.kwargs {
            visit.call(kwargs)?;
        }
        if let Some(args) = &self.last_args {
            visit.call(args)?;
        }
        if let Some(kwargs) = &self.last_kwargs {
            visit.call(kwargs)?;
        }
        if let Some(greenlet) = &self.greenlet {
            visit.call(greenlet)?;
        }
        if let Some(pending_exception) = &self.pending_exception {
            pending_exception.traverse(&visit)?;
        }
        if let Some(channel) = &self.blocked_channel {
            visit.call(channel)?;
        }
        if let Some(context_manager_getter) = &self.context_manager_getter {
            visit.call(context_manager_getter)?;
        }
        if let Some(exception_handler) = &self.exception_handler {
            visit.call(exception_handler)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        self.callable = None;
        self.args = None;
        self.kwargs = None;
        self.last_args = None;
        self.last_kwargs = None;
        self.greenlet = None;
        self.pending_exception = None;
        self.blocked_channel = None;
        self.blocked_direction = None;
        self.context_manager_getter = None;
        self.exception_handler = None;
        self.callable_bound = false;
        self.alive = false;
        self.blocked = false;
        self.scheduled = false;
        self.paused = false;
        self.continuation_pending = false;
        self.kill_pending = false;
        self.sync_core_state();
    }

    fn insert(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<PyObject> {
        ensure_switch_allowed()?;
        let tasklet = (&slf).into_py(py);
        if slf.is_main {
            return Ok(tasklet);
        }
        if slf.blocked {
            return Err(PyRuntimeError::new_err(
                "Failed to insert tasklet: Cannot insert blocked tasklet",
            ));
        }
        if !slf.alive {
            return Err(PyRuntimeError::new_err(
                "Failed to insert tasklet: Cannot insert dead tasklet",
            ));
        }
        if slf.callable.is_none() {
            return Err(PyRuntimeError::new_err("tasklet has no callable"));
        }
        if !slf.scheduled {
            slf.alive = true;
            slf.paused = false;
            slf.scheduled = true;
            bridge_core_resume_tasklet(slf.core_id);
            bridge_core_set_tasklet_block_trap(slf.core_id, slf.block_trap);
            queue_tasklet_core_for_thread(
                py,
                slf.owner_thread,
                slf.core_id,
                tasklet.clone_ref(py),
            )?;
        }
        slf.sync_core_state();
        Ok(tasklet)
    }

    fn remove(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<PyObject> {
        let tasklet = (&slf).into_py(py);
        remove_queued_tasklet_by_core_id(slf.owner_thread, slf.core_id, slf.as_ptr());
        if slf.callable.is_some() {
            slf.alive = true;
            slf.paused = true;
            slf.continuation_pending = false;
            slf.kill_pending = false;
            slf.pending_exception = None;
        }
        slf.blocked = false;
        slf.scheduled = false;
        slf.blocked_channel = None;
        slf.blocked_direction = None;
        if slf.paused {
            bridge_core_pause_tasklet(slf.core_id);
            bridge_core_set_tasklet_block_trap(slf.core_id, slf.block_trap);
        } else {
            slf.sync_core_state();
        }
        Ok(tasklet)
    }

    #[pyo3(signature = (pending=false))]
    fn kill(slf: PyRefMut<'_, Self>, py: Python<'_>, pending: bool) -> PyResult<()> {
        ensure_switch_allowed()?;
        let tasklet = (&slf).into_py(py);
        drop(slf);
        kill_tasklet_object(py, tasklet, pending)?;
        let queued = queued_tasklet_count();
        if !pending && queued > 0 {
            run_queued_tasklets(py, queued)?;
        }
        Ok(())
    }

    #[pyo3(signature = (func=None, args=None, kwargs=None))]
    fn bind(
        mut slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        func: Option<PyObject>,
        args: Option<PyObject>,
        kwargs: Option<PyObject>,
    ) -> PyResult<PyObject> {
        let tasklet = (&slf).into_py(py);
        let args_are_bound = args.is_some() || kwargs.is_some();
        if !slf.belongs_to_current_thread() {
            let message = if func.is_none() && !args_are_bound {
                "Failed to unbind tasklet: Cannot unbind tasklet from another thread"
            } else {
                "Failed to bind tasklet: Cannot bind tasklet from another thread"
            };
            return Err(PyRuntimeError::new_err(message));
        }

        let bound_args = if let Some(args) = args {
            if args.bind(py).downcast::<PyTuple>().is_err() {
                return Err(PyTypeError::new_err("tasklet args must be a tuple"));
            }
            Some(args)
        } else if kwargs.is_some() {
            Some(PyTuple::empty_bound(py).to_object(py))
        } else {
            None
        };

        let bound_kwargs = if let Some(kwargs) = kwargs {
            if kwargs.bind(py).downcast::<PyDict>().is_err() {
                return Err(PyTypeError::new_err("tasklet kwargs must be a dict"));
            }
            Some(kwargs)
        } else {
            None
        };

        if func.is_none() && !args_are_bound {
            if slf.is_main {
                return Err(PyRuntimeError::new_err("can't unbind the current tasklet"));
            }
            if slf.scheduled {
                return Err(PyRuntimeError::new_err("can't unbind a scheduled tasklet"));
            }
            slf.callable = None;
            slf.args = None;
            slf.kwargs = None;
            if let Some(greenlet) = slf.greenlet.take() {
                dispose_tasklet_greenlet(py, &greenlet);
            }
            slf.callable_bound = false;
            slf.alive = false;
            slf.blocked = false;
            slf.paused = false;
            slf.continuation_pending = false;
            slf.kill_pending = false;
            slf.pending_exception = None;
            slf.blocked_channel = None;
            slf.blocked_direction = None;
            slf.times_switched_to = 0;
            slf.sync_core_state();
            return Ok(tasklet);
        }

        if slf.scheduled {
            if !slf.continuation_pending || (!func.is_some() && !args_are_bound) {
                return Err(PyRuntimeError::new_err("can't bind a scheduled tasklet"));
            }
            remove_queued_tasklet_by_core_id(slf.owner_thread, slf.core_id, tasklet.as_ptr());
            slf.scheduled = false;
        }

        if let Some(func) = func {
            slf.bind_callable(py, func)?;
        } else if slf.callable.is_none() {
            return Err(PyRuntimeError::new_err("tasklet has no callable"));
        }

        slf.args = bound_args;
        slf.kwargs = bound_kwargs;
        if let Some(greenlet) = slf.greenlet.take() {
            dispose_tasklet_greenlet(py, &greenlet);
        }
        slf.blocked = false;
        slf.paused = args_are_bound;
        slf.continuation_pending = false;
        slf.kill_pending = false;
        slf.pending_exception = None;
        slf.blocked_channel = None;
        slf.blocked_direction = None;
        slf.alive = slf.paused;
        if slf.paused {
            bridge_core_pause_tasklet(slf.core_id);
            bridge_core_set_tasklet_block_trap(slf.core_id, slf.block_trap);
        } else {
            slf.sync_core_state();
        }
        Ok(tasklet)
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        mut slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        if slf.scheduled {
            return Err(PyRuntimeError::new_err("tasklet is already scheduled"));
        }
        if slf.callable.is_none() {
            return Err(PyRuntimeError::new_err("tasklet has no callable"));
        }

        slf.args = Some(args.to_object(py));
        slf.kwargs = kwargs.map(|kwargs| kwargs.to_object(py));
        slf.greenlet = None;
        slf.scheduled = true;
        slf.alive = true;
        slf.paused = false;
        slf.continuation_pending = false;
        slf.kill_pending = false;
        slf.pending_exception = None;
        slf.blocked_channel = None;
        slf.blocked_direction = None;
        slf.sync_core_state();

        let tasklet = (&slf).into_py(py);
        queue_tasklet_core_for_thread(py, slf.owner_thread, slf.core_id, tasklet.clone_ref(py))?;
        Ok(tasklet)
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn setup(
        slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        let tasklet = (&slf).into_py(py);
        if !slf.belongs_to_current_thread() {
            return Err(PyRuntimeError::new_err(
                "Failed to setup tasklet: Cannot setup tasklet from another thread",
            ));
        }
        drop(slf);
        enqueue_tasklet_object(
            py,
            tasklet.clone_ref(py),
            args.to_object(py),
            kwargs.map(|kwargs| kwargs.to_object(py)),
        )?;
        Ok(tasklet)
    }

    fn run(slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<()> {
        ensure_switch_allowed()?;
        if !slf.alive {
            return Err(PyRuntimeError::new_err(
                "Cannot run tasklet that is not alive (dead)",
            ));
        }
        if slf.blocked {
            return Err(PyRuntimeError::new_err(
                "Cannot run tasklet that is blocked",
            ));
        }
        if !slf.belongs_to_current_thread() {
            return Ok(());
        }
        let tasklet = slf.as_ptr();
        drop(slf);
        run_tasklet_by_ptr(py, tasklet)
    }

    fn switch(slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<()> {
        if !slf.alive {
            return Err(PyRuntimeError::new_err("tasklet is dead"));
        }
        if slf.blocked {
            return Err(PyRuntimeError::new_err("tasklet is blocked"));
        }
        ensure_switch_allowed()?;
        if !slf.belongs_to_current_thread() {
            return Err(PyRuntimeError::new_err(
                "Failed to switch tasklet: Cannot switch tasklet from another thread",
            ));
        }
        let tasklet = slf.as_ptr();
        drop(slf);
        let source = current_tasklet_object(py)?;
        set_tasklet_paused(py, &source, true);
        let result = run_tasklet_by_ptr(py, tasklet);
        set_tasklet_paused(py, &source, false);
        result
    }

    #[pyo3(signature = (exception, arguments=None))]
    fn raise_exception(
        slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        exception: PyObject,
        arguments: Option<PyObject>,
    ) -> PyResult<()> {
        ensure_switch_allowed()?;
        validate_exception_type_or_instance(
            py,
            &exception,
            "Exception type or instance required",
            ExceptionValidationError::Runtime,
        )?;

        let tasklet = (&slf).into_py(py);
        drop(slf);

        let value = arguments.unwrap_or_else(|| py.None());
        throw_exception_into_tasklet(py, tasklet, TaskletException::set(exception, value), false)
    }

    #[pyo3(signature = (exc, val=None, tb=None, pending=false))]
    fn throw(
        slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        exc: PyObject,
        val: Option<PyObject>,
        tb: Option<PyObject>,
        pending: bool,
    ) -> PyResult<()> {
        ensure_switch_allowed()?;
        let value = val.unwrap_or_else(|| py.None());
        validate_tasklet_throw_exception_setup(py, &exc, &value)?;

        let tasklet = (&slf).into_py(py);
        drop(slf);

        let exception = if let Some(traceback) = tb {
            TaskletException::restore(exc, value, traceback)
        } else {
            TaskletException::set(exc, value)
        };
        throw_exception_into_tasklet(py, tasklet, exception, pending)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChannelBlockDirection {
    Send,
    Receive,
}

const INVALID_CHANNEL_ERROR: &str =
    "Channel object is not valid. Most likely cause being __init__ not called on base type.";

#[pyclass(name = "channel", module = "_scheduler", subclass, weakref)]
pub struct Channel {
    initialized: bool,
    core_id: Option<CoreChannelId>,
    object_ptr: usize,
    preference: i32,
    balance: i32,
    messages: VecDeque<ChannelPayload>,
    blocked_senders: VecDeque<PyObject>,
    blocked_receivers: VecDeque<PyObject>,
    closed: bool,
    closing: bool,
}

struct ChannelPayload {
    token: CorePayloadToken,
    message: ChannelMessage,
}

enum ChannelMessage {
    Value(PyObject),
    SetException {
        exc: PyObject,
        value: PyObject,
    },
    RestoreException {
        exc: PyObject,
        value: PyObject,
        traceback: PyObject,
    },
}

impl ChannelMessage {
    fn into_receive_result(self, py: Python<'_>) -> PyResult<PyObject> {
        match self {
            Self::Value(value) => Ok(value),
            Self::SetException { exc, value } => Err(raise_python_exception(py, exc, value, None)),
            Self::RestoreException {
                exc,
                value,
                traceback,
            } => Err(raise_python_exception(py, exc, value, Some(traceback))),
        }
    }
}

impl Channel {
    fn uninitialized() -> Self {
        Self {
            initialized: false,
            core_id: None,
            object_ptr: 0,
            preference: -1,
            balance: 0,
            messages: VecDeque::new(),
            blocked_senders: VecDeque::new(),
            blocked_receivers: VecDeque::new(),
            closed: false,
            closing: false,
        }
    }

    fn reset_initialized_state(&mut self, object_ptr: Option<*mut ffi::PyObject>) {
        if !self.initialized {
            ACTIVE_CHANNELS.fetch_add(1, Ordering::SeqCst);
        }
        self.initialized = true;
        if let Some(object_ptr) = object_ptr {
            self.register_object_ptr(object_ptr);
        }
        self.preference = -1;
        self.balance = 0;
        self.messages.clear();
        self.blocked_senders.clear();
        self.blocked_receivers.clear();
        self.closed = false;
        self.closing = false;
        let core_id = self
            .core_id
            .unwrap_or_else(|| bridge_core_create_channel(self.preference));
        self.core_id = Some(core_id);
        bridge_core_reset_channel(core_id, self.preference);
    }

    fn register_object_ptr(&mut self, object_ptr: *mut ffi::PyObject) {
        if object_ptr.is_null() {
            return;
        }
        if self.object_ptr != 0 {
            return;
        }
        self.object_ptr = object_ptr as usize;
        ACTIVE_CHANNEL_OBJECTS
            .lock()
            .expect("active channel registry lock poisoned")
            .push(self.object_ptr);
    }

    fn ensure_valid(&self) -> PyResult<()> {
        if self.initialized {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err(INVALID_CHANNEL_ERROR))
        }
    }

    fn core_channel_id(&self) -> PyResult<CoreChannelId> {
        self.core_id
            .ok_or_else(|| PyRuntimeError::new_err("scheduler channel has no Rust core handle"))
    }

    fn core_snapshot(&self) -> PyResult<CoreChannelSnapshot> {
        bridge_core_channel_snapshot(self.core_channel_id()?)
    }

    fn core_balance(&self) -> PyResult<i32> {
        let snapshot = self.core_snapshot()?;
        i32::try_from(snapshot.balance)
            .map_err(|_| PyRuntimeError::new_err("scheduler channel balance overflowed i32"))
    }

    fn core_preference(&self) -> PyResult<i32> {
        let snapshot = self.core_snapshot()?;
        i32::try_from(snapshot.preference)
            .map_err(|_| PyRuntimeError::new_err("scheduler channel preference overflowed i32"))
    }

    fn set_balance_from_core(&mut self, balance: i64) -> PyResult<()> {
        self.balance = i32::try_from(balance)
            .map_err(|_| PyRuntimeError::new_err("scheduler channel balance overflowed i32"))?;
        Ok(())
    }

    fn refresh_balance_from_core(&mut self) {
        if let Some(core_id) = self.core_id {
            if let Ok(snapshot) = bridge_core_channel_snapshot(core_id) {
                let _ = self.set_balance_from_core(snapshot.balance);
            }
        }
    }

    fn push_payload(&mut self, token: CorePayloadToken, message: ChannelMessage) {
        self.messages.push_back(ChannelPayload { token, message });
    }

    fn pop_payload_by_token(&mut self, token: CorePayloadToken) -> PyResult<ChannelMessage> {
        let index = self
            .messages
            .iter()
            .position(|payload| payload.token == token)
            .ok_or_else(|| {
                PyRuntimeError::new_err(
                    "scheduler core bridge selected a missing channel payload token",
                )
            })?;
        self.messages
            .remove(index)
            .map(|payload| payload.message)
            .ok_or_else(|| {
                PyRuntimeError::new_err("scheduler core bridge failed to remove channel payload")
            })
    }

    fn remove_payload_by_token(&mut self, token: CorePayloadToken) {
        if let Some(index) = self
            .messages
            .iter()
            .position(|payload| payload.token == token)
        {
            self.messages.remove(index);
        }
    }

    fn assert_core_mirror(&self) -> PyResult<()> {
        #[cfg(debug_assertions)]
        {
            let snapshot = self.core_snapshot()?;
            debug_assert_eq!(snapshot.preference, i64::from(self.preference));
            debug_assert_eq!(snapshot.balance, i64::from(self.balance));
        }
        Ok(())
    }

    fn pop_blocked_receiver_by_core_id(
        &mut self,
        py: Python<'_>,
        receiver: CoreTaskletId,
    ) -> PyResult<PyObject> {
        let index = self
            .blocked_receivers
            .iter()
            .position(|candidate| tasklet_core_id(py, candidate).ok() == Some(receiver))
            .ok_or_else(|| {
                PyRuntimeError::new_err("scheduler core bridge matched a missing receiver tasklet")
            })?;
        self.blocked_receivers.remove(index).ok_or_else(|| {
            PyRuntimeError::new_err("scheduler core bridge failed to remove receiver tasklet")
        })
    }

    fn pop_blocked_sender_by_core_id(
        &mut self,
        py: Python<'_>,
        sender: CoreTaskletId,
    ) -> PyResult<PyObject> {
        let index = self
            .blocked_senders
            .iter()
            .position(|candidate| tasklet_core_id(py, candidate).ok() == Some(sender))
            .ok_or_else(|| {
                PyRuntimeError::new_err("scheduler core bridge matched a missing sender tasklet")
            })?;
        self.blocked_senders.remove(index).ok_or_else(|| {
            PyRuntimeError::new_err("scheduler core bridge failed to remove sender tasklet")
        })
    }

    fn blocked_tasklet_by_core_id(
        &self,
        py: Python<'_>,
        tasklet: CoreTaskletId,
    ) -> PyResult<Option<PyObject>> {
        for candidate in self
            .blocked_receivers
            .iter()
            .chain(self.blocked_senders.iter())
        {
            if tasklet_core_id(py, candidate).ok() == Some(tasklet) {
                return Ok(Some(candidate.clone_ref(py)));
            }
        }
        Ok(None)
    }

    fn update_close_state(&mut self) {
        if self.closing && self.balance == 0 && self.messages.is_empty() {
            self.closed = true;
        }
    }

    fn clear_blocked_tasklets(&mut self, py: Python<'_>) {
        if let Some(core_id) = self.core_id {
            let _ = bridge_core_clear_channel(core_id);
        }
        let tasklets = self
            .blocked_receivers
            .drain(..)
            .chain(self.blocked_senders.drain(..))
            .collect::<Vec<_>>();
        self.messages.clear();
        self.balance = 0;
        self.update_close_state();
        for tasklet in tasklets {
            deliver_tasklet_exit_to_blocked_tasklet(py, &tasklet);
        }
    }

    fn remove_blocked_tasklets_for_thread(
        &mut self,
        py: Python<'_>,
        thread_id: ThreadId,
    ) -> Vec<PyObject> {
        let mut removed = Vec::new();
        let mut receivers = VecDeque::new();
        while let Some(receiver) = self.blocked_receivers.pop_front() {
            if tasklet_belongs_to_thread(py, &receiver, thread_id) {
                if let Ok(core_id) = tasklet_core_id(py, &receiver) {
                    let _ = bridge_core_remove_tasklet_from_channel(core_id);
                    self.refresh_balance_from_core();
                } else {
                    self.balance += 1;
                }
                removed.push(receiver);
            } else {
                receivers.push_back(receiver);
            }
        }
        self.blocked_receivers = receivers;

        let mut senders = VecDeque::new();
        while let Some(sender) = self.blocked_senders.pop_front() {
            if tasklet_belongs_to_thread(py, &sender, thread_id) {
                if let Ok(core_id) = tasklet_core_id(py, &sender) {
                    if let Some(payload_token) = bridge_core_remove_tasklet_from_channel(core_id) {
                        self.remove_payload_by_token(payload_token);
                    }
                    self.refresh_balance_from_core();
                } else {
                    self.balance -= 1;
                }
                removed.push(sender);
            } else {
                senders.push_back(sender);
            }
        }
        self.blocked_senders = senders;

        self.update_close_state();
        removed
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        if self.initialized {
            if self.object_ptr != 0 {
                let mut channels = ACTIVE_CHANNEL_OBJECTS
                    .lock()
                    .expect("active channel registry lock poisoned");
                channels.retain(|object_ptr| *object_ptr != self.object_ptr);
            }
            ACTIVE_CHANNELS.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[pymethods]
impl Channel {
    #[new]
    #[classmethod]
    #[pyo3(signature = (_owner=None))]
    fn new(cls: &Bound<'_, PyType>, _owner: Option<PyObject>) -> Self {
        let mut channel = Self::uninitialized();
        let is_base_channel = cls
            .getattr("__name__")
            .and_then(|name| name.extract::<String>())
            .is_ok_and(|name| name == "channel");
        if is_base_channel {
            channel.reset_initialized_state(None);
        }
        channel
    }

    #[pyo3(signature = (_owner=None))]
    fn __init__(mut slf: PyRefMut<'_, Self>, _owner: Option<PyObject>) {
        let object_ptr = slf.as_ptr();
        slf.reset_initialized_state(Some(object_ptr));
    }

    #[getter]
    fn preference(&self) -> PyResult<i32> {
        self.ensure_valid()?;
        self.core_preference()
    }

    #[setter]
    fn set_preference(&mut self, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        self.ensure_valid()?;
        let Some(value) = value else {
            return Err(PyTypeError::new_err("Cannot delete the first attribute"));
        };
        if !value.is_instance_of::<PyLong>() {
            return Err(PyTypeError::new_err(
                "The first attribute value must be a number",
            ));
        }
        let preference = value.extract::<i32>()?;
        if (-1..=1).contains(&preference) {
            self.preference = preference;
            bridge_core_set_channel_preference(self.core_channel_id()?, preference)?;
        }
        Ok(())
    }

    fn __setattr__(
        mut slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        name: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if name == "preference" {
            slf.set_preference(Some(value))
        } else {
            let name = PyString::new_bound(py, name);
            let result = unsafe {
                ffi::PyObject_GenericSetAttr(slf.as_ptr(), name.as_ptr(), value.as_ptr())
            };
            if result == 0 {
                Ok(())
            } else {
                Err(PyErr::fetch(py))
            }
        }
    }

    fn __delattr__(slf: PyRef<'_, Self>, py: Python<'_>, name: &str) -> PyResult<()> {
        if name == "preference" {
            slf.ensure_valid()?;
            Err(PyTypeError::new_err("Cannot delete the first attribute"))
        } else {
            let name = PyString::new_bound(py, name);
            let result = unsafe {
                ffi::PyObject_GenericSetAttr(slf.as_ptr(), name.as_ptr(), ptr::null_mut())
            };
            if result == 0 {
                Ok(())
            } else {
                Err(PyErr::fetch(py).into())
            }
        }
    }

    #[getter]
    fn balance(&self) -> PyResult<i32> {
        self.ensure_valid()?;
        self.assert_core_mirror()?;
        self.core_balance()
    }

    #[getter]
    fn queue(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.ensure_valid()?;
        if let Some(front) = bridge_core_queue_front(self.core_channel_id()?)? {
            self.blocked_tasklet_by_core_id(py, front)?.ok_or_else(|| {
                PyRuntimeError::new_err("scheduler core bridge selected a missing queue tasklet")
            })
        } else {
            Ok(py.None())
        }
    }

    #[getter]
    fn closed(&self) -> PyResult<bool> {
        self.ensure_valid()?;
        Ok(self.core_snapshot()?.closed)
    }

    #[getter]
    fn closing(&self) -> PyResult<bool> {
        self.ensure_valid()?;
        Ok(self.core_snapshot()?.closing)
    }

    #[pyo3(signature = (*args))]
    fn send(slf: PyRefMut<'_, Self>, py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        slf.ensure_valid()?;
        if args.len() != 1 {
            return Err(PyTypeError::new_err(
                "Channel.send() takes exactly one argument",
            ));
        }
        let value = args.get_item(0)?.to_object(py);
        let channel = (&slf).into_py(py);
        drop(slf);
        send_channel_message(py, channel, ChannelMessage::Value(value))
    }

    fn receive(slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<PyObject> {
        slf.ensure_valid()?;
        let channel = (&slf).into_py(py);
        drop(slf);
        receive_channel_message(py, channel)
    }

    #[pyo3(signature = (*args))]
    fn send_exception(
        slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<()> {
        slf.ensure_valid()?;
        if args.is_empty() {
            return Err(PyRuntimeError::new_err("Exception type required"));
        }
        let exc = args.get_item(0)?.to_object(py);
        validate_exception_type_or_instance(
            py,
            &exc,
            "Exception type or instance required",
            ExceptionValidationError::Runtime,
        )?;
        let value = exception_value_from_args(py, args, 1)?;
        let channel = (&slf).into_py(py);
        drop(slf);
        send_channel_message(py, channel, ChannelMessage::SetException { exc, value })
    }

    #[pyo3(signature = (exc, val=None, tb=None))]
    fn send_throw(
        slf: PyRefMut<'_, Self>,
        py: Python<'_>,
        exc: PyObject,
        val: Option<PyObject>,
        tb: Option<PyObject>,
    ) -> PyResult<()> {
        slf.ensure_valid()?;
        validate_exception_type_or_instance(
            py,
            &exc,
            "Channel.send_throw() argument 'exc' (pos 1) must be an Exception type or instance",
            ExceptionValidationError::Type,
        )?;
        let value = val.unwrap_or_else(|| py.None());
        let traceback = tb.unwrap_or_else(|| py.None());
        let channel = (&slf).into_py(py);
        drop(slf);
        send_channel_message(
            py,
            channel,
            ChannelMessage::RestoreException {
                exc,
                value,
                traceback,
            },
        )
    }

    fn clear(&mut self, py: Python<'_>) -> PyResult<()> {
        self.ensure_valid()?;
        self.clear_blocked_tasklets(py);
        self.assert_core_mirror()?;
        Ok(())
    }

    fn close(&mut self) -> PyResult<()> {
        self.ensure_valid()?;
        bridge_core_close_channel(self.core_channel_id()?)?;
        self.closing = true;
        self.update_close_state();
        self.assert_core_mirror()?;
        Ok(())
    }

    fn open(&mut self) -> PyResult<()> {
        self.ensure_valid()?;
        bridge_core_open_channel(self.core_channel_id()?)?;
        self.closing = false;
        self.closed = false;
        self.assert_core_mirror()?;
        Ok(())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<PyObject> {
        slf.ensure_valid()?;
        if slf.closed && slf.messages.is_empty() {
            return Err(PyStopIteration::new_err("Channel is closed"));
        }
        Self::receive(slf, py)
    }
}

#[pyclass(name = "schedule_manager", module = "_scheduler", subclass, weakref)]
#[derive(Debug, Clone)]
pub struct ScheduleManager;

#[pymethods]
impl ScheduleManager {
    #[new]
    fn new() -> Self {
        Self
    }
}

#[pyfunction]
fn abi_version() -> u32 {
    carbon_scheduler_abi_version()
}

#[pyfunction]
fn core_status() -> &'static str {
    match carbon_scheduler_ffi::carbon_scheduler_core_status() {
        CarbonSchedulerStatus::Ok => "ok",
        CarbonSchedulerStatus::InvalidHandle => "invalid_handle",
        CarbonSchedulerStatus::Unsupported => "unsupported",
        CarbonSchedulerStatus::Panic => "panic",
    }
}

#[pyfunction]
fn getcurrent(py: Python<'_>) -> PyResult<PyObject> {
    current_tasklet_object(py)
}

#[pyfunction]
fn getmain(py: Python<'_>) -> PyResult<PyObject> {
    Ok(current_tasklet(py)?.to_object(py))
}

#[pyfunction]
fn getruncount() -> u32 {
    1 + queued_tasklet_count() as u32 + executing_tasklet_count() as u32
}

#[pyfunction]
fn calculateruncount() -> u32 {
    getruncount()
}

#[pyfunction]
fn run(py: Python<'_>) -> PyResult<()> {
    ensure_switch_allowed()?;
    while queued_tasklet_count() > 0 {
        run_queued_tasklets(py, 1)?;
    }
    Ok(())
}

#[pyfunction]
fn run_n_tasklets(py: Python<'_>, number_of_tasklets: u32) -> PyResult<()> {
    ensure_switch_allowed()?;
    run_queued_tasklets(py, number_of_tasklets as usize)?;
    Ok(())
}

#[pyfunction]
fn schedule(py: Python<'_>) -> PyResult<()> {
    ensure_switch_allowed()?;
    if let Some(tasklet) = executing_tasklet(py) {
        {
            let tasklet_ref = tasklet
                .bind(py)
                .downcast::<Tasklet>()
                .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
            let mut tasklet_ref = tasklet_ref.try_borrow_mut()?;
            if !tasklet_ref.scheduled {
                tasklet_ref.alive = true;
                tasklet_ref.blocked = false;
                tasklet_ref.scheduled = true;
                tasklet_ref.paused = false;
                tasklet_ref.continuation_pending = true;
                let owner_thread = tasklet_ref.owner_thread;
                let core_id = tasklet_ref.core_id;
                queue_tasklet_core_for_thread(py, owner_thread, core_id, tasklet.clone_ref(py))?;
            }
        }
        if yield_current_greenlet(py)? {
            return Ok(());
        }
        return Err(TaskletPaused::new_err("tasklet paused by schedule"));
    }

    if queued_tasklet_count() == 0 {
        return Ok(());
    }

    let current = current_tasklet(py)?;
    {
        let mut current = current.bind(py).try_borrow_mut()?;
        current.scheduled = true;
        bridge_core_set_tasklet_scheduled_snapshot(current.core_id, true);
    }
    let result = run_queued_tasklets(py, 1).map(|_| ());
    {
        let mut current = current.bind(py).try_borrow_mut()?;
        current.scheduled = false;
        bridge_core_set_tasklet_scheduled_snapshot(current.core_id, false);
    }
    result
}

#[pyfunction]
fn schedule_remove(py: Python<'_>) -> PyResult<()> {
    ensure_switch_allowed()?;
    if let Some(tasklet) = executing_tasklet(py) {
        if queued_tasklet_count() > 0 {
            run_queued_tasklets(py, 1)?;
        }
        mark_tasklet_paused(py, &tasklet);
        if yield_current_greenlet(py)? {
            return Ok(());
        }
        return Err(TaskletPaused::new_err("tasklet paused by schedule_remove"));
    }

    if queued_tasklet_count() > 0 {
        run_queued_tasklets(py, 1)?;
    }
    Ok(())
}

#[pyfunction]
fn get_schedule_manager(py: Python<'_>) -> PyResult<Py<ScheduleManager>> {
    schedule_manager(py)
}

#[pyfunction]
fn get_number_of_active_schedule_managers() -> u32 {
    ACTIVE_SCHEDULE_MANAGERS.load(Ordering::SeqCst)
}

#[pyfunction]
fn get_number_of_active_channels() -> u32 {
    ACTIVE_CHANNELS.load(Ordering::SeqCst)
}

#[pyfunction]
fn unblock_all_channels(py: Python<'_>) -> u32 {
    let active_channels = ACTIVE_CHANNEL_OBJECTS
        .lock()
        .expect("active channel registry lock poisoned")
        .clone();
    let mut unblocked = 0;

    for object_ptr in active_channels {
        let Some(channel_object) = borrowed_any_from_ptr(py, object_ptr as *mut ffi::PyObject)
        else {
            continue;
        };
        let Ok(channel) = channel_object.downcast::<Channel>() else {
            continue;
        };
        let Ok(mut channel) = channel.try_borrow_mut() else {
            continue;
        };
        if channel.initialized && channel.balance != 0 {
            channel.clear_blocked_tasklets(py);
            unblocked += 1;
        }
    }

    unblocked
}

#[pyfunction]
fn set_use_nested_tasklets(value: bool) {
    USE_NESTED_TASKLETS.store(value, Ordering::SeqCst);
}

#[pyfunction]
fn get_use_nested_tasklets() -> bool {
    USE_NESTED_TASKLETS.load(Ordering::SeqCst)
}

#[pyfunction]
fn get_all_time_tasklet_count() -> u32 {
    ALL_TIME_TASKLETS.load(Ordering::SeqCst)
}

#[pyfunction]
fn get_active_tasklet_count() -> u32 {
    ACTIVE_TASKLETS.load(Ordering::SeqCst)
}

#[pyfunction]
fn enable_soft_switch(value: Option<PyObject>) -> PyResult<bool> {
    if value.is_some() {
        Err(PyRuntimeError::new_err(
            "enable_soft_switch is only implemented for legacy reasons, the value cannot be changed.",
        ))
    } else {
        Ok(false)
    }
}

#[pyfunction]
fn enable_softswitch(value: Option<PyObject>) -> PyResult<bool> {
    enable_soft_switch(value)
}

#[pyfunction]
fn set_channel_callback(py: Python<'_>, callback: PyObject) -> PyResult<PyObject> {
    let callback = validated_callback_replacement(py, callback)?;
    CHANNEL_CALLBACK_TOUCHED.with(|touched| {
        *touched.borrow_mut() = true;
    });
    CHANNEL_CALLBACK.with(|slot| {
        let mut slot = slot.borrow_mut();
        let previous = slot.take().unwrap_or_else(|| py.None());
        *slot = callback;
        Ok(previous)
    })
}

#[pyfunction]
fn get_channel_callback(py: Python<'_>) -> PyObject {
    CHANNEL_CALLBACK.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|callback| callback.clone_ref(py))
            .unwrap_or_else(|| py.None())
    })
}

#[pyfunction]
fn set_schedule_callback(py: Python<'_>, callback: PyObject) -> PyResult<PyObject> {
    let callback = validated_callback_replacement(py, callback)?;
    Ok(replace_schedule_callback(py, callback))
}

#[pyfunction]
fn get_schedule_callback(py: Python<'_>) -> PyObject {
    schedule_callback(py).unwrap_or_else(|| py.None())
}

#[pyfunction]
fn switch_trap(delta: i32) -> PyResult<i32> {
    let Some(core_queue_id) = current_thread_core_run_queue_id(delta != 0) else {
        return Ok(0);
    };
    Ok(bridge_core_switch_trap(core_queue_id, delta)? as i32)
}

#[pyfunction]
fn get_thread_info(py: Python<'_>) -> PyResult<PyObject> {
    let main = getmain(py)?;
    let current = getcurrent(py)?;
    Ok((main, current, getruncount() + 1).into_py(py))
}

#[pymodule]
fn _scheduler(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    populate_scheduler_module(py, module)
}

#[pymodule]
fn _scheduler_debug(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    populate_scheduler_module(py, module)
}

#[pymodule]
fn _scheduler_trinitydev(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    populate_scheduler_module(py, module)
}

#[pymodule]
fn _scheduler_internal(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    populate_scheduler_module(py, module)
}

pub fn populate_scheduler_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("callable_wrapper", py.get_type_bound::<CallableWrapper>())?;
    module.add_class::<TaskletGreenletRunner>()?;
    module.add_class::<ThreadCleanupSentinel>()?;
    module.add_class::<Tasklet>()?;
    module.add_class::<Channel>()?;
    module.add_class::<ScheduleManager>()?;
    module.add("TaskletExit", py.get_type_bound::<TaskletExit>())?;
    module.add("_C_API", create_scheduler_c_api_capsule(py)?)?;
    module.add_function(wrap_pyfunction!(abi_version, module)?)?;
    module.add_function(wrap_pyfunction!(bridge_status, module)?)?;
    module.add_function(wrap_pyfunction!(core_status, module)?)?;
    module.add_function(wrap_pyfunction!(getcurrent, module)?)?;
    module.add_function(wrap_pyfunction!(getmain, module)?)?;
    module.add_function(wrap_pyfunction!(getruncount, module)?)?;
    module.add_function(wrap_pyfunction!(calculateruncount, module)?)?;
    module.add_function(wrap_pyfunction!(run, module)?)?;
    module.add_function(wrap_pyfunction!(run_n_tasklets, module)?)?;
    module.add_function(wrap_pyfunction!(schedule, module)?)?;
    module.add_function(wrap_pyfunction!(schedule_remove, module)?)?;
    module.add_function(wrap_pyfunction!(get_schedule_manager, module)?)?;
    module.add_function(wrap_pyfunction!(
        get_number_of_active_schedule_managers,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(get_number_of_active_channels, module)?)?;
    module.add_function(wrap_pyfunction!(unblock_all_channels, module)?)?;
    module.add_function(wrap_pyfunction!(set_use_nested_tasklets, module)?)?;
    module.add_function(wrap_pyfunction!(get_use_nested_tasklets, module)?)?;
    module.add_function(wrap_pyfunction!(get_all_time_tasklet_count, module)?)?;
    module.add_function(wrap_pyfunction!(get_active_tasklet_count, module)?)?;
    module.add_function(wrap_pyfunction!(enable_soft_switch, module)?)?;
    module.add_function(wrap_pyfunction!(enable_softswitch, module)?)?;
    module.add_function(wrap_pyfunction!(set_channel_callback, module)?)?;
    module.add_function(wrap_pyfunction!(get_channel_callback, module)?)?;
    module.add_function(wrap_pyfunction!(set_schedule_callback, module)?)?;
    module.add_function(wrap_pyfunction!(get_schedule_callback, module)?)?;
    module.add_function(wrap_pyfunction!(switch_trap, module)?)?;
    module.add_function(wrap_pyfunction!(get_thread_info, module)?)?;
    Ok(())
}

fn validated_callback_replacement(
    py: Python<'_>,
    callback: PyObject,
) -> PyResult<Option<PyObject>> {
    if callback.bind(py).is_none() {
        Ok(None)
    } else if callback.bind(py).is_callable() {
        Ok(Some(callback))
    } else {
        Err(PyTypeError::new_err("parameter must be callable or None."))
    }
}

enum ExceptionValidationError {
    Runtime,
    Type,
}

fn validate_exception_type_or_instance(
    py: Python<'_>,
    object: &PyObject,
    message: &'static str,
    error_kind: ExceptionValidationError,
) -> PyResult<()> {
    let object = object.bind(py);
    let is_exception_class = unsafe { ffi::PyExceptionClass_Check(object.as_ptr()) != 0 };
    if is_exception_class || object.is_instance(&py.get_type_bound::<PyException>())? {
        Ok(())
    } else {
        match error_kind {
            ExceptionValidationError::Runtime => Err(PyRuntimeError::new_err(message)),
            ExceptionValidationError::Type => Err(PyTypeError::new_err(message)),
        }
    }
}

fn validate_tasklet_throw_exception_setup(
    py: Python<'_>,
    exception: &PyObject,
    value: &PyObject,
) -> PyResult<()> {
    let exception = exception.bind(py);
    if exception.is_instance(&py.get_type_bound::<PyException>())? {
        if value.bind(py).is_none() {
            Ok(())
        } else {
            Err(PyTypeError::new_err(
                "missing required argument 'exc' (pos 1)",
            ))
        }
    } else if exception.is_none() {
        Err(PyTypeError::new_err(
            "missing required argument 'exc' (pos 1)",
        ))
    } else if unsafe { ffi::PyExceptionClass_Check(exception.as_ptr()) != 0 } {
        Ok(())
    } else {
        Err(PyTypeError::new_err(
            "exceptions must be classes, or instances",
        ))
    }
}

fn py_object_into_nullable_ptr(py: Python<'_>, object: PyObject) -> *mut ffi::PyObject {
    if object.bind(py).is_none() {
        ptr::null_mut()
    } else {
        object.into_ptr()
    }
}

fn raise_python_exception(
    py: Python<'_>,
    exc: PyObject,
    value: PyObject,
    traceback: Option<PyObject>,
) -> PyErr {
    let exc_is_instance = exc.bind(py).downcast::<PyBaseException>().is_ok();
    if exc_is_instance && traceback.is_none() {
        PyErr::from_value_bound(exc.bind(py).clone()).restore(py);
        return PyErr::fetch(py);
    }

    unsafe {
        if exc_is_instance {
            let exc_type = exc.bind(py).get_type().to_object(py);
            let traceback = traceback.map_or(ptr::null_mut(), |traceback| {
                py_object_into_nullable_ptr(py, traceback)
            });
            #[allow(deprecated)]
            ffi::PyErr_Restore(exc_type.into_ptr(), exc.into_ptr(), traceback);
        } else if let Some(traceback) = traceback {
            let value = py_object_into_nullable_ptr(py, value);
            let traceback = py_object_into_nullable_ptr(py, traceback);
            #[allow(deprecated)]
            ffi::PyErr_Restore(exc.into_ptr(), value, traceback);
        } else {
            ffi::PyErr_SetObject(exc.as_ptr(), value.as_ptr());
        }
    }
    PyErr::fetch(py)
}

fn exception_value_from_args(
    py: Python<'_>,
    args: &Bound<'_, PyTuple>,
    start: usize,
) -> PyResult<PyObject> {
    match args.len().saturating_sub(start) {
        0 => Ok(py.None()),
        1 => Ok(args.get_item(start)?.to_object(py)),
        _ => Ok(args.get_slice(start, args.len()).to_object(py)),
    }
}

fn channel_object_from_ptr(py: Python<'_>, channel: *mut ffi::PyObject) -> PyResult<PyObject> {
    let object = borrowed_any_from_ptr(py, channel)
        .ok_or_else(|| PyTypeError::new_err("expected scheduler.channel"))?;
    if object.downcast::<Channel>().is_err() {
        return Err(PyTypeError::new_err("expected scheduler.channel"));
    }
    Ok(object.to_object(py))
}

fn channel_callback(py: Python<'_>) -> Option<PyObject> {
    CHANNEL_CALLBACK.with(|callback| {
        callback
            .borrow()
            .as_ref()
            .map(|callback| callback.clone_ref(py))
    })
}

fn call_channel_callback(
    py: Python<'_>,
    channel: &PyObject,
    tasklet: &PyObject,
    sending: bool,
    will_block: bool,
) -> PyResult<()> {
    if !CHANNEL_CALLBACK_TOUCHED.with(|touched| *touched.borrow()) {
        return Ok(());
    }
    if let Ok(tasklet_ref) = tasklet.bind(py).downcast::<Tasklet>() {
        if let Ok(mut tasklet_ref) = tasklet_ref.try_borrow_mut() {
            if tasklet_ref.skip_next_channel_callback {
                tasklet_ref.skip_next_channel_callback = false;
                return Ok(());
            }
        }
    }
    if let Some(callback) = channel_callback(py) {
        callback.call1(py, (channel, tasklet, sending, will_block))?;
    }
    Ok(())
}

fn send_channel_message(
    py: Python<'_>,
    channel_object: PyObject,
    message: ChannelMessage,
) -> PyResult<()> {
    ensure_switch_allowed()?;
    let will_block = {
        let channel = channel_object
            .bind(py)
            .downcast::<Channel>()
            .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
        let channel = channel.try_borrow()?;
        channel.ensure_valid()?;
        channel.blocked_receivers.is_empty()
    };
    let current = current_tasklet_object(py)?;
    let current_core_id = tasklet_core_id(py, &current)?;
    call_channel_callback(py, &channel_object, &current, true, will_block)?;

    let channel = channel_object
        .bind(py)
        .downcast::<Channel>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
    let mut channel = channel.try_borrow_mut()?;
    channel.ensure_valid()?;
    channel.register_object_ptr(channel_object.as_ptr());
    let channel_core_id = channel.core_channel_id()?;

    if !channel.blocked_receivers.is_empty() {
        let (receiver, receiver_runs_immediately, payload_token) =
            match bridge_core_send(current_core_id, channel_core_id)? {
                CoreChannelOperationResult::Matched {
                    receiver,
                    payload_token,
                    preferred,
                    peer_runs_immediately,
                    balance,
                    ..
                } => {
                    if peer_runs_immediately && preferred != receiver {
                        return Err(PyRuntimeError::new_err(
                            "scheduler core bridge selected an unexpected immediate send peer",
                        ));
                    }
                    channel.set_balance_from_core(balance)?;
                    (
                        channel.pop_blocked_receiver_by_core_id(py, receiver)?,
                        peer_runs_immediately,
                        payload_token,
                    )
                }
                CoreChannelOperationResult::Blocked { .. } => {
                    return Err(PyRuntimeError::new_err(
                        "scheduler core bridge expected channel send to match",
                    ));
                }
            };
        channel.push_payload(payload_token, message);
        channel.update_close_state();
        channel.assert_core_mirror()?;
        drop(channel);
        continue_channel_tasklet_after_transfer(py, &receiver, receiver_runs_immediately)?;
        return Ok(());
    }

    if channel.closed || channel.closing {
        return Err(PyValueError::new_err("Send operation on a closed channel"));
    }

    if current_tasklet_block_trap(py)? {
        return Err(PyRuntimeError::new_err(
            "This tasklet does not allow blocking.",
        ));
    }

    if let Some(tasklet) = executing_tasklet(py) {
        match bridge_core_send(current_core_id, channel_core_id)? {
            CoreChannelOperationResult::Blocked {
                tasklet,
                direction,
                balance,
                payload_token,
                ..
            } if tasklet == current_core_id && direction == CoreChannelDirection::Send => {
                channel.set_balance_from_core(balance)?;
                let payload_token = payload_token.ok_or_else(|| {
                    PyRuntimeError::new_err(
                        "scheduler core bridge blocked send without a payload token",
                    )
                })?;
                channel.push_payload(payload_token, message);
            }
            CoreChannelOperationResult::Blocked { .. } => {
                return Err(PyRuntimeError::new_err(
                    "scheduler core bridge blocked an unexpected tasklet on send",
                ));
            }
            CoreChannelOperationResult::Matched { .. } => {
                return Err(PyRuntimeError::new_err(
                    "scheduler core bridge expected channel send to block",
                ));
            }
        }
        channel.blocked_senders.push_back(tasklet.clone_ref(py));
        mark_tasklet_blocked(
            py,
            &tasklet,
            Some(channel_object.clone_ref(py)),
            Some(ChannelBlockDirection::Send),
        );
        channel.assert_core_mirror()?;
        drop(channel);
        if yield_current_greenlet(py)? {
            return Ok(());
        }
        return Err(TaskletBlocked::new_err("tasklet blocked on channel send"));
    }

    drop(channel);
    run_until_channel_progress_or_deadlock(py)?;
    let channel = channel_object
        .bind(py)
        .downcast::<Channel>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
    let mut channel = channel.try_borrow_mut()?;
    channel.ensure_valid()?;
    let channel_core_id = channel.core_channel_id()?;
    if channel.blocked_receivers.is_empty() {
        return Err(PyRuntimeError::new_err(
            "Deadlock: channel send would block",
        ));
    };
    let (receiver, receiver_runs_immediately, payload_token) =
        match bridge_core_send(current_core_id, channel_core_id)? {
            CoreChannelOperationResult::Matched {
                receiver,
                payload_token,
                preferred,
                peer_runs_immediately,
                balance,
                ..
            } => {
                if peer_runs_immediately && preferred != receiver {
                    return Err(PyRuntimeError::new_err(
                        "scheduler core bridge selected an unexpected immediate send peer",
                    ));
                }
                channel.set_balance_from_core(balance)?;
                (
                    channel.pop_blocked_receiver_by_core_id(py, receiver)?,
                    peer_runs_immediately,
                    payload_token,
                )
            }
            CoreChannelOperationResult::Blocked { .. } => {
                return Err(PyRuntimeError::new_err(
                    "scheduler core bridge expected channel send to match",
                ));
            }
        };
    channel.push_payload(payload_token, message);
    channel.update_close_state();
    channel.assert_core_mirror()?;
    drop(channel);
    continue_channel_tasklet_after_transfer(py, &receiver, receiver_runs_immediately)?;
    Ok(())
}

fn run_sender_preferred_receiver_before_receive(
    py: Python<'_>,
    channel_object: &PyObject,
) -> PyResult<()> {
    if executing_tasklet(py).is_none() || queued_tasklet_count() == 0 {
        return Ok(());
    }

    let should_run_receiver = {
        let channel = channel_object
            .bind(py)
            .downcast::<Channel>()
            .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
        let channel = channel.try_borrow()?;
        channel.ensure_valid()?;
        channel.preference >= 1
            && !channel.messages.is_empty()
            && channel.blocked_senders.is_empty()
    };

    if should_run_receiver {
        run_queued_tasklets(py, 1)?;
    }
    Ok(())
}

fn receive_channel_message(py: Python<'_>, channel_object: PyObject) -> PyResult<PyObject> {
    ensure_switch_allowed()?;
    run_sender_preferred_receiver_before_receive(py, &channel_object)?;
    let will_block = {
        let channel = channel_object
            .bind(py)
            .downcast::<Channel>()
            .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
        let channel = channel.try_borrow()?;
        channel.ensure_valid()?;
        channel.blocked_senders.is_empty() && channel.messages.is_empty()
    };
    let current = current_tasklet_object(py)?;
    let current_core_id = tasklet_core_id(py, &current)?;
    call_channel_callback(py, &channel_object, &current, false, will_block)?;

    let channel = channel_object
        .bind(py)
        .downcast::<Channel>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
    let mut channel = channel.try_borrow_mut()?;
    channel.ensure_valid()?;
    channel.register_object_ptr(channel_object.as_ptr());
    let channel_core_id = channel.core_channel_id()?;
    if (channel.closed || channel.closing) && channel.messages.is_empty() {
        return Err(PyValueError::new_err(
            "receive operation on a closed channel",
        ));
    }

    if let Some(payload_token) = bridge_core_take_tasklet_payload_token(current_core_id)? {
        let message = channel.pop_payload_by_token(payload_token)?;
        channel.update_close_state();
        channel.assert_core_mirror()?;
        drop(channel);
        return message.into_receive_result(py);
    }

    if !channel.blocked_senders.is_empty() {
        let (message, sender, sender_runs_immediately) =
            match bridge_core_receive(current_core_id, channel_core_id)? {
                CoreChannelOperationResult::Matched {
                    sender,
                    payload_token,
                    preferred,
                    peer_runs_immediately,
                    balance,
                    ..
                } => {
                    if peer_runs_immediately && preferred != sender {
                        return Err(PyRuntimeError::new_err(
                            "scheduler core bridge selected an unexpected immediate receive peer",
                        ));
                    }
                    channel.set_balance_from_core(balance)?;
                    (
                        channel.pop_payload_by_token(payload_token)?,
                        channel.pop_blocked_sender_by_core_id(py, sender)?,
                        peer_runs_immediately,
                    )
                }
                CoreChannelOperationResult::Blocked { .. } => {
                    return Err(PyRuntimeError::new_err(
                        "scheduler core bridge expected channel receive to match",
                    ));
                }
            };
        channel.update_close_state();
        channel.assert_core_mirror()?;
        drop(channel);
        continue_channel_tasklet_after_transfer(py, &sender, sender_runs_immediately)?;
        return message.into_receive_result(py);
    }

    if current_tasklet_block_trap(py)? {
        return Err(PyRuntimeError::new_err(
            "This tasklet does not allow blocking.",
        ));
    }

    if let Some(tasklet) = executing_tasklet(py) {
        match bridge_core_receive(current_core_id, channel_core_id)? {
            CoreChannelOperationResult::Blocked {
                tasklet,
                direction,
                balance,
                ..
            } if tasklet == current_core_id && direction == CoreChannelDirection::Receive => {
                channel.set_balance_from_core(balance)?;
            }
            CoreChannelOperationResult::Blocked { .. } => {
                return Err(PyRuntimeError::new_err(
                    "scheduler core bridge blocked an unexpected tasklet on receive",
                ));
            }
            CoreChannelOperationResult::Matched { .. } => {
                return Err(PyRuntimeError::new_err(
                    "scheduler core bridge expected channel receive to block",
                ));
            }
        }
        channel.blocked_receivers.push_back(tasklet.clone_ref(py));
        mark_tasklet_blocked(
            py,
            &tasklet,
            Some(channel_object.clone_ref(py)),
            Some(ChannelBlockDirection::Receive),
        );
        channel.assert_core_mirror()?;
        drop(channel);
        if !yield_current_greenlet(py)? {
            return Err(TaskletBlocked::new_err(
                "tasklet blocked on channel receive",
            ));
        }
        let channel = channel_object
            .bind(py)
            .downcast::<Channel>()
            .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
        let mut channel = channel.try_borrow_mut()?;
        channel.ensure_valid()?;
        let Some(payload_token) = bridge_core_take_tasklet_payload_token(current_core_id)? else {
            return Err(PyRuntimeError::new_err(
                "Deadlock: channel receive would block",
            ));
        };
        let message = channel.pop_payload_by_token(payload_token)?;
        channel.update_close_state();
        channel.assert_core_mirror()?;
        drop(channel);
        return message.into_receive_result(py);
    }

    drop(channel);
    run_until_channel_progress_or_deadlock(py)?;
    let channel = channel_object
        .bind(py)
        .downcast::<Channel>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
    let mut channel = channel.try_borrow_mut()?;
    channel.ensure_valid()?;
    let channel_core_id = channel.core_channel_id()?;
    if !channel.blocked_senders.is_empty() {
        let (message, sender, sender_runs_immediately) =
            match bridge_core_receive(current_core_id, channel_core_id)? {
                CoreChannelOperationResult::Matched {
                    sender,
                    payload_token,
                    preferred,
                    peer_runs_immediately,
                    balance,
                    ..
                } => {
                    if peer_runs_immediately && preferred != sender {
                        return Err(PyRuntimeError::new_err(
                            "scheduler core bridge selected an unexpected immediate receive peer",
                        ));
                    }
                    channel.set_balance_from_core(balance)?;
                    (
                        channel.pop_payload_by_token(payload_token)?,
                        channel.pop_blocked_sender_by_core_id(py, sender)?,
                        peer_runs_immediately,
                    )
                }
                CoreChannelOperationResult::Blocked { .. } => {
                    return Err(PyRuntimeError::new_err(
                        "scheduler core bridge expected channel receive to match",
                    ));
                }
            };
        channel.update_close_state();
        channel.assert_core_mirror()?;
        drop(channel);
        continue_channel_tasklet_after_transfer(py, &sender, sender_runs_immediately)?;
        return message.into_receive_result(py);
    }
    if let Some(payload_token) = bridge_core_take_tasklet_payload_token(current_core_id)? {
        let message = channel.pop_payload_by_token(payload_token)?;
        channel.update_close_state();
        channel.assert_core_mirror()?;
        drop(channel);
        return message.into_receive_result(py);
    }
    Err(PyRuntimeError::new_err(
        "Deadlock: channel receive would block",
    ))
}

fn inject_channel_exception_for_blocked_receiver(
    py: Python<'_>,
    channel_object: PyObject,
    tasklet_object: PyObject,
    tasklet_ptr: *mut ffi::PyObject,
    message: ChannelMessage,
    scheduled: bool,
) -> PyResult<()> {
    let channel = channel_object
        .bind(py)
        .downcast::<Channel>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
    let mut channel = channel.try_borrow_mut()?;
    channel.ensure_valid()?;

    let before = channel.blocked_receivers.len();
    channel
        .blocked_receivers
        .retain(|receiver| receiver.as_ptr() != tasklet_ptr);
    if channel.blocked_receivers.len() == before {
        return Err(PyRuntimeError::new_err(
            "tasklet is not blocked on this channel receive",
        ));
    }

    let core_id = tasklet_core_id(py, &tasklet_object)?;
    let _ = bridge_core_remove_tasklet_from_channel(core_id);
    let payload_token = bridge_core_assign_tasklet_payload_token(core_id)?;
    channel.push_payload(payload_token, message);
    channel.refresh_balance_from_core();
    channel.update_close_state();
    channel.assert_core_mirror()?;
    drop(channel);

    prepare_tasklet_for_channel_replay(py, &tasklet_object, scheduled);
    if scheduled {
        queue_tasklet_for_owner(py, &tasklet_object)?;
    }
    Ok(())
}

fn remove_tasklet_from_blocked_channel(
    py: Python<'_>,
    channel_object: PyObject,
    tasklet_ptr: *mut ffi::PyObject,
    direction: Option<ChannelBlockDirection>,
) -> PyResult<()> {
    let channel = channel_object
        .bind(py)
        .downcast::<Channel>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.channel"))?;
    let mut channel = channel.try_borrow_mut()?;
    channel.ensure_valid()?;
    match direction {
        Some(ChannelBlockDirection::Receive) => {
            if let Some(index) = channel
                .blocked_receivers
                .iter()
                .position(|receiver| receiver.as_ptr() == tasklet_ptr)
            {
                let tasklet_object = channel.blocked_receivers.remove(index);
                if let Some(tasklet_object) = tasklet_object {
                    if let Ok(core_id) = tasklet_core_id(py, &tasklet_object) {
                        let _ = bridge_core_remove_tasklet_from_channel(core_id);
                        channel.refresh_balance_from_core();
                    } else {
                        channel.balance += 1;
                    }
                }
            }
        }
        Some(ChannelBlockDirection::Send) => {
            if let Some(index) = channel
                .blocked_senders
                .iter()
                .position(|sender| sender.as_ptr() == tasklet_ptr)
            {
                let tasklet_object = channel.blocked_senders.remove(index);
                if let Some(tasklet_object) = tasklet_object {
                    if let Ok(core_id) = tasklet_core_id(py, &tasklet_object) {
                        if let Some(payload_token) =
                            bridge_core_remove_tasklet_from_channel(core_id)
                        {
                            channel.remove_payload_by_token(payload_token);
                        }
                        channel.refresh_balance_from_core();
                    } else {
                        channel.balance -= 1;
                    }
                }
            }
        }
        None => {}
    }
    channel.update_close_state();
    channel.assert_core_mirror()?;
    Ok(())
}

fn throw_exception_into_tasklet(
    py: Python<'_>,
    tasklet_object: PyObject,
    exception: TaskletException,
    pending: bool,
) -> PyResult<()> {
    if executing_tasklet(py).is_some_and(|current| current.as_ptr() == tasklet_object.as_ptr()) {
        return Err(exception.raise(py));
    }

    let tasklet = tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
    let tasklet_ptr = tasklet.as_ptr();
    let tasklet_exit = exception.is_tasklet_exit(py);
    let (blocked_channel, blocked_direction, alive, blocked, scheduled) = {
        let tasklet = tasklet.try_borrow()?;
        if !tasklet.belongs_to_current_thread() {
            return Err(PyRuntimeError::new_err(
                "Failed to throw tasklet: Cannot throw tasklet from another thread",
            ));
        }
        (
            tasklet
                .blocked_channel
                .as_ref()
                .map(|channel| channel.clone_ref(py)),
            tasklet.blocked_direction,
            tasklet.alive,
            tasklet.blocked,
            tasklet.scheduled,
        )
    };

    if !alive {
        remove_queued_tasklet(py, tasklet_ptr);
        if tasklet_exit {
            return Ok(());
        }
        return Err(PyRuntimeError::new_err(
            "You cannot throw to a dead tasklet",
        ));
    }

    if blocked && matches!(blocked_direction, Some(ChannelBlockDirection::Receive)) {
        if let Some(channel) = blocked_channel.as_ref() {
            inject_channel_exception_for_blocked_receiver(
                py,
                channel.clone_ref(py),
                tasklet_object.clone_ref(py),
                tasklet_ptr,
                exception.into_channel_message(),
                pending,
            )?;
            if pending {
                return Ok(());
            }
            return execute_tasklet_object(py, &tasklet_object);
        }
    }
    if blocked {
        if let Some(channel) = blocked_channel {
            remove_tasklet_from_blocked_channel(py, channel, tasklet_ptr, blocked_direction)?;
        }
    }

    {
        let mut tasklet = tasklet.try_borrow_mut()?;
        tasklet.pending_exception = Some(exception);
        tasklet.alive = true;
        tasklet.blocked = false;
        tasklet.paused = false;
        tasklet.continuation_pending = false;
        tasklet.kill_pending = false;
        tasklet.blocked_channel = None;
        tasklet.blocked_direction = None;
        if !scheduled {
            tasklet.scheduled = true;
            queue_tasklet_core_for_thread(
                py,
                tasklet.owner_thread,
                tasklet.core_id,
                tasklet_object.clone_ref(py),
            )?;
        }
        tasklet.sync_core_state();
    }

    if pending {
        Ok(())
    } else {
        run_tasklet_by_ptr(py, tasklet_ptr)
    }
}

fn c_int_from_py_result(py: Python<'_>, result: PyResult<()>) -> i32 {
    match result {
        Ok(()) => 0,
        Err(error) => {
            error.restore(py);
            -1
        }
    }
}

fn legacy_std_string_from_py_object(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if !value.is_instance_of::<PyString>() {
        return Err(PyTypeError::new_err("value must be a string"));
    }
    value.extract()
}

#[repr(C)]
struct SchedulerCapsuleApi {
    py_tasklet_new: *mut c_void,
    py_tasklet_setup: *mut c_void,
    py_tasklet_insert: *mut c_void,
    py_tasklet_get_block_trap: *mut c_void,
    py_tasklet_set_block_trap: *mut c_void,
    py_tasklet_is_main: *mut c_void,
    py_tasklet_check: *mut c_void,
    py_tasklet_alive: *mut c_void,
    py_tasklet_kill: *mut c_void,
    py_channel_new: *mut c_void,
    py_channel_send: *mut c_void,
    py_channel_receive: *mut c_void,
    py_channel_send_exception: *mut c_void,
    py_channel_get_queue: *mut c_void,
    py_channel_get_preference: *mut c_void,
    py_channel_set_preference: *mut c_void,
    py_channel_get_balance: *mut c_void,
    py_channel_check: *mut c_void,
    py_channel_send_throw: *mut c_void,
    py_scheduler_get_scheduler: *mut c_void,
    py_scheduler_schedule: *mut c_void,
    py_scheduler_get_run_count: *mut c_void,
    py_scheduler_get_current: *mut c_void,
    py_scheduler_run_with_timeout: *mut c_void,
    py_scheduler_run_n_tasklets: *mut c_void,
    py_scheduler_set_channel_callback: *mut c_void,
    py_scheduler_get_channel_callback: *mut c_void,
    py_scheduler_set_schedule_callback: *mut c_void,
    py_scheduler_set_schedule_fast_callback: *mut c_void,
    py_scheduler_get_number_of_active_schedule_managers: *mut c_void,
    py_scheduler_get_number_of_active_channels: *mut c_void,
    py_scheduler_get_all_time_tasklet_count: *mut c_void,
    py_scheduler_get_active_tasklet_count: *mut c_void,
    py_scheduler_get_tasklets_completed_last_run_with_timeout: *mut c_void,
    py_scheduler_get_tasklets_switched_last_run_with_timeout: *mut c_void,
    py_tasklet_type: *mut ffi::PyTypeObject,
    py_channel_type: *mut ffi::PyTypeObject,
    tasklet_exit: *mut *mut ffi::PyObject,
    py_tasklet_get_times_switched_to: *mut c_void,
    py_tasklet_get_context: *mut c_void,
}

impl SchedulerCapsuleApi {
    const fn empty() -> Self {
        Self {
            py_tasklet_new: ptr::null_mut(),
            py_tasklet_setup: ptr::null_mut(),
            py_tasklet_insert: ptr::null_mut(),
            py_tasklet_get_block_trap: ptr::null_mut(),
            py_tasklet_set_block_trap: ptr::null_mut(),
            py_tasklet_is_main: ptr::null_mut(),
            py_tasklet_check: ptr::null_mut(),
            py_tasklet_alive: ptr::null_mut(),
            py_tasklet_kill: ptr::null_mut(),
            py_channel_new: ptr::null_mut(),
            py_channel_send: ptr::null_mut(),
            py_channel_receive: ptr::null_mut(),
            py_channel_send_exception: ptr::null_mut(),
            py_channel_get_queue: ptr::null_mut(),
            py_channel_get_preference: ptr::null_mut(),
            py_channel_set_preference: ptr::null_mut(),
            py_channel_get_balance: ptr::null_mut(),
            py_channel_check: ptr::null_mut(),
            py_channel_send_throw: ptr::null_mut(),
            py_scheduler_get_scheduler: ptr::null_mut(),
            py_scheduler_schedule: ptr::null_mut(),
            py_scheduler_get_run_count: ptr::null_mut(),
            py_scheduler_get_current: ptr::null_mut(),
            py_scheduler_run_with_timeout: ptr::null_mut(),
            py_scheduler_run_n_tasklets: ptr::null_mut(),
            py_scheduler_set_channel_callback: ptr::null_mut(),
            py_scheduler_get_channel_callback: ptr::null_mut(),
            py_scheduler_set_schedule_callback: ptr::null_mut(),
            py_scheduler_set_schedule_fast_callback: ptr::null_mut(),
            py_scheduler_get_number_of_active_schedule_managers: ptr::null_mut(),
            py_scheduler_get_number_of_active_channels: ptr::null_mut(),
            py_scheduler_get_all_time_tasklet_count: ptr::null_mut(),
            py_scheduler_get_active_tasklet_count: ptr::null_mut(),
            py_scheduler_get_tasklets_completed_last_run_with_timeout: ptr::null_mut(),
            py_scheduler_get_tasklets_switched_last_run_with_timeout: ptr::null_mut(),
            py_tasklet_type: ptr::null_mut(),
            py_channel_type: ptr::null_mut(),
            tasklet_exit: ptr::null_mut(),
            py_tasklet_get_times_switched_to: ptr::null_mut(),
            py_tasklet_get_context: ptr::null_mut(),
        }
    }
}

struct SchedulerCapsuleApiStorage(UnsafeCell<SchedulerCapsuleApi>);

unsafe impl Sync for SchedulerCapsuleApiStorage {}

impl SchedulerCapsuleApiStorage {
    const fn new() -> Self {
        Self(UnsafeCell::new(SchedulerCapsuleApi::empty()))
    }

    fn as_mut_ptr(&self) -> *mut SchedulerCapsuleApi {
        self.0.get()
    }
}

struct TaskletExitApiStorage(UnsafeCell<*mut ffi::PyObject>);

unsafe impl Sync for TaskletExitApiStorage {}

impl TaskletExitApiStorage {
    const fn new() -> Self {
        Self(UnsafeCell::new(ptr::null_mut()))
    }

    fn as_mut_ptr(&self) -> *mut *mut ffi::PyObject {
        self.0.get()
    }
}

static SCHEDULER_CAPSULE_API: SchedulerCapsuleApiStorage = SchedulerCapsuleApiStorage::new();
static SCHEDULER_TASKLET_EXIT: TaskletExitApiStorage = TaskletExitApiStorage::new();

fn scheduler_capsule_name_ptr() -> *const c_char {
    SCHEDULER_CAPSULE_C_NAME.as_ptr().cast::<c_char>()
}

fn py_object_c_api_ptr(function: extern "C" fn() -> *mut ffi::PyObject) -> *mut c_void {
    function as *const () as *mut c_void
}

fn py_object_py_type_py_object_c_api_ptr(
    function: extern "C" fn(*mut ffi::PyTypeObject, *mut ffi::PyObject) -> *mut ffi::PyObject,
) -> *mut c_void {
    function as *const () as *mut c_void
}

fn py_object_py_type_c_api_ptr(
    function: extern "C" fn(*mut ffi::PyTypeObject) -> *mut ffi::PyObject,
) -> *mut c_void {
    function as *const () as *mut c_void
}

fn int_c_api_ptr(function: extern "C" fn() -> i32) -> *mut c_void {
    function as *const () as *mut c_void
}

fn int_py_object_c_api_ptr(function: extern "C" fn(*mut ffi::PyObject) -> i32) -> *mut c_void {
    function as *const () as *mut c_void
}

fn int_py_object_py_object_c_api_ptr(
    function: extern "C" fn(*mut ffi::PyObject, *mut ffi::PyObject) -> i32,
) -> *mut c_void {
    function as *const () as *mut c_void
}

fn int_py_object_py_object_py_object_c_api_ptr(
    function: extern "C" fn(*mut ffi::PyObject, *mut ffi::PyObject, *mut ffi::PyObject) -> i32,
) -> *mut c_void {
    function as *const () as *mut c_void
}

fn int_py_object_py_object_py_object_py_object_c_api_ptr(
    function: extern "C" fn(
        *mut ffi::PyObject,
        *mut ffi::PyObject,
        *mut ffi::PyObject,
        *mut ffi::PyObject,
    ) -> i32,
) -> *mut c_void {
    function as *const () as *mut c_void
}

fn void_py_object_int_c_api_ptr(function: extern "C" fn(*mut ffi::PyObject, i32)) -> *mut c_void {
    function as *const () as *mut c_void
}

fn void_schedule_hook_c_api_ptr(function: extern "C" fn(Option<ScheduleHookFunc>)) -> *mut c_void {
    function as *const () as *mut c_void
}

fn py_object_int_c_api_ptr(function: extern "C" fn(i32) -> *mut ffi::PyObject) -> *mut c_void {
    function as *const () as *mut c_void
}

fn py_object_i64_c_api_ptr(function: extern "C" fn(i64) -> *mut ffi::PyObject) -> *mut c_void {
    function as *const () as *mut c_void
}

fn py_object_py_object_int_c_api_ptr(
    function: extern "C" fn(*mut ffi::PyObject, i32) -> *mut ffi::PyObject,
) -> *mut c_void {
    function as *const () as *mut c_void
}

fn py_object_py_object_c_api_ptr(
    function: extern "C" fn(*mut ffi::PyObject) -> *mut ffi::PyObject,
) -> *mut c_void {
    function as *const () as *mut c_void
}

fn long_py_object_c_api_ptr(function: extern "C" fn(*mut ffi::PyObject) -> c_long) -> *mut c_void {
    function as *const () as *mut c_void
}

fn cstr_py_object_c_api_ptr(
    function: extern "C" fn(*mut ffi::PyObject) -> *const c_char,
) -> *mut c_void {
    function as *const () as *mut c_void
}

fn borrowed_any_from_ptr<'py>(
    py: Python<'py>,
    object: *mut ffi::PyObject,
) -> Option<Bound<'py, PyAny>> {
    unsafe { Bound::<PyAny>::from_borrowed_ptr_or_opt(py, object) }
}

fn ensure_thread_cleanup(py: Python<'_>) -> PyResult<()> {
    CURRENT_THREAD_RUN_QUEUE.with(|_| {});
    THREAD_CLEANUP_GUARD.with(|_| {});
    let Some(thread_dict) =
        (unsafe { Bound::<PyAny>::from_borrowed_ptr_or_opt(py, ffi::PyThreadState_GetDict()) })
    else {
        return Ok(());
    };
    let thread_dict = thread_dict.downcast::<PyDict>()?;
    if !thread_dict.contains(THREAD_CLEANUP_SENTINEL_KEY)? {
        let sentinel = Py::new(py, ThreadCleanupSentinel::new())?;
        thread_dict.set_item(THREAD_CLEANUP_SENTINEL_KEY, sentinel)?;
    }
    Ok(())
}

fn current_thread_id() -> ThreadId {
    std::thread::current().id()
}

fn bridge_core_scheduler() -> &'static Mutex<CoreScheduler> {
    BRIDGE_CORE_SCHEDULER.get_or_init(|| Mutex::new(CoreScheduler::new()))
}

fn bridge_core_error(error: CoreSchedulerHandleError) -> PyErr {
    PyRuntimeError::new_err(format!("scheduler core bridge error: {error}"))
}

fn with_bridge_core<T>(
    operation: impl FnOnce(&mut CoreScheduler) -> Result<T, CoreSchedulerHandleError>,
) -> PyResult<T> {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("scheduler core bridge lock poisoned"))?;
    operation(&mut scheduler).map_err(bridge_core_error)
}

fn bridge_core_create_tasklet() -> CoreTaskletId {
    bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned")
        .create_tasklet()
}

fn bridge_core_create_channel(preference: i32) -> CoreChannelId {
    bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned")
        .create_channel(i64::from(preference))
}

fn bridge_core_create_run_queue() -> CoreRunQueueId {
    bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned")
        .create_run_queue()
}

fn bridge_core_switch_trap(queue: CoreRunQueueId, delta: i32) -> PyResult<i64> {
    with_bridge_core(|scheduler| scheduler.switch_trap(queue, i64::from(delta)))
}

fn bridge_core_is_switch_trapped(queue: CoreRunQueueId) -> PyResult<bool> {
    with_bridge_core(|scheduler| scheduler.is_switch_trapped(queue))
}

fn bridge_core_reset_channel(channel: CoreChannelId, preference: i32) {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned");
    let _ = scheduler.clear_channel(channel);
    let _ = scheduler.open_channel(channel);
    let _ = scheduler.set_channel_preference(channel, i64::from(preference));
}

fn bridge_core_set_tasklet_block_trap(tasklet: CoreTaskletId, value: bool) {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned");
    let _ = scheduler.set_tasklet_block_trap(tasklet, value);
}

fn bridge_core_sync_tasklet_state(
    tasklet: CoreTaskletId,
    alive: bool,
    paused: bool,
    times_switched_to: u64,
    block_trap: bool,
) {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned");
    let _ = scheduler.update_tasklet_runtime_state(tasklet, alive, paused, times_switched_to);
    let _ = scheduler.set_tasklet_block_trap(tasklet, block_trap);
}

fn bridge_core_pause_tasklet(tasklet: CoreTaskletId) {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned");
    let _ = scheduler.pause_tasklet(tasklet);
}

fn bridge_core_resume_tasklet(tasklet: CoreTaskletId) {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned");
    let _ = scheduler.resume_tasklet(tasklet);
}

fn bridge_core_set_tasklet_scheduled_snapshot(tasklet: CoreTaskletId, scheduled: bool) {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned");
    let _ = scheduler.set_tasklet_scheduled_snapshot(tasklet, scheduled);
}

fn bridge_core_schedule_tasklet_back(
    queue: CoreRunQueueId,
    tasklet: CoreTaskletId,
) -> PyResult<()> {
    with_bridge_core(|scheduler| scheduler.schedule_tasklet_back(queue, tasklet))
}

fn bridge_core_remove_runnable_tasklet(tasklet: CoreTaskletId) {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned");
    let _ = scheduler.remove_runnable_tasklet(tasklet);
}

fn bridge_core_pop_next_runnable_tasklet(queue: CoreRunQueueId) -> PyResult<Option<CoreTaskletId>> {
    with_bridge_core(|scheduler| scheduler.pop_next_runnable_tasklet(queue))
}

fn bridge_core_clear_run_queue(queue: CoreRunQueueId) -> PyResult<Vec<CoreTaskletId>> {
    with_bridge_core(|scheduler| scheduler.clear_run_queue(queue))
}

fn bridge_core_tasklet_snapshot(tasklet: CoreTaskletId) -> PyResult<CoreTaskletSnapshot> {
    with_bridge_core(|scheduler| scheduler.tasklet_snapshot(tasklet))
}

fn bridge_core_set_channel_preference(channel: CoreChannelId, preference: i32) -> PyResult<()> {
    with_bridge_core(|scheduler| scheduler.set_channel_preference(channel, i64::from(preference)))
}

fn bridge_core_close_channel(channel: CoreChannelId) -> PyResult<()> {
    with_bridge_core(|scheduler| scheduler.close_channel(channel))
}

fn bridge_core_open_channel(channel: CoreChannelId) -> PyResult<()> {
    with_bridge_core(|scheduler| scheduler.open_channel(channel))
}

fn bridge_core_clear_channel(channel: CoreChannelId) -> PyResult<Vec<CoreTaskletId>> {
    with_bridge_core(|scheduler| scheduler.clear_channel(channel))
}

fn bridge_core_remove_tasklet_from_channel(tasklet: CoreTaskletId) -> Option<CorePayloadToken> {
    let mut scheduler = bridge_core_scheduler()
        .lock()
        .expect("scheduler core bridge lock poisoned");
    scheduler
        .remove_tasklet_from_channel(tasklet)
        .ok()
        .flatten()
}

fn bridge_core_channel_snapshot(channel: CoreChannelId) -> PyResult<CoreChannelSnapshot> {
    with_bridge_core(|scheduler| scheduler.channel_snapshot(channel))
}

fn bridge_core_queue_front(channel: CoreChannelId) -> PyResult<Option<CoreTaskletId>> {
    with_bridge_core(|scheduler| scheduler.queue_front(channel))
}

fn bridge_core_send(
    sender: CoreTaskletId,
    channel: CoreChannelId,
) -> PyResult<CoreChannelOperationResult> {
    with_bridge_core(|scheduler| scheduler.send(sender, channel))
}

fn bridge_core_receive(
    receiver: CoreTaskletId,
    channel: CoreChannelId,
) -> PyResult<CoreChannelOperationResult> {
    with_bridge_core(|scheduler| scheduler.receive(receiver, channel))
}

fn bridge_core_take_tasklet_payload_token(
    tasklet: CoreTaskletId,
) -> PyResult<Option<CorePayloadToken>> {
    with_bridge_core(|scheduler| scheduler.take_tasklet_payload_token(tasklet))
}

fn bridge_core_assign_tasklet_payload_token(tasklet: CoreTaskletId) -> PyResult<CorePayloadToken> {
    with_bridge_core(|scheduler| scheduler.assign_tasklet_payload_token(tasklet))
}

fn tasklet_core_id(py: Python<'_>, tasklet_object: &PyObject) -> PyResult<CoreTaskletId> {
    let tasklet = tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
    Ok(tasklet.try_borrow()?.core_id)
}

fn tasklet_owner_thread(py: Python<'_>, tasklet_object: &PyObject) -> Option<ThreadId> {
    tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .ok()
        .and_then(|tasklet| {
            tasklet
                .try_borrow()
                .ok()
                .map(|tasklet| tasklet.owner_thread)
        })
}

fn tasklet_belongs_to_thread(
    py: Python<'_>,
    tasklet_object: &PyObject,
    thread_id: ThreadId,
) -> bool {
    tasklet_owner_thread(py, tasklet_object).is_some_and(|owner| owner == thread_id)
}

fn thread_run_queue_index(queues: &[ThreadRunQueue], thread_id: ThreadId) -> Option<usize> {
    queues.iter().position(|entry| entry.thread_id == thread_id)
}

fn cached_run_queue_count(generation: u64) -> Option<usize> {
    RUN_QUEUE_COUNT_CACHE.with(|cache| {
        cache.borrow().and_then(|(cached_generation, count)| {
            (cached_generation == generation).then_some(count)
        })
    })
}

fn set_cached_run_queue_count(generation: u64, count: usize) {
    RUN_QUEUE_COUNT_CACHE.with(|cache| {
        *cache.borrow_mut() = Some((generation, count));
    });
}

fn current_queue_len_from_locked(queues: &[ThreadRunQueue], thread_id: ThreadId) -> usize {
    queues
        .iter()
        .find(|entry| entry.thread_id == thread_id)
        .map_or(0, |entry| entry.queue.len())
}

fn publish_run_queue_count_change(thread_id: ThreadId, count: usize) {
    let generation = RUN_QUEUE_GENERATION.fetch_add(1, Ordering::AcqRel) + 1;
    if thread_id == current_thread_id() {
        set_cached_run_queue_count(generation, count);
    }
}

fn publish_foreign_run_queue_change(thread_id: ThreadId, count: usize) {
    FOREIGN_RUN_QUEUE_GENERATION.fetch_add(1, Ordering::AcqRel);
    publish_run_queue_count_change(thread_id, count);
}

fn set_seen_foreign_run_queue_generation(generation: u64) {
    FOREIGN_RUN_QUEUE_SEEN.with(|seen| {
        *seen.borrow_mut() = generation;
    });
}

fn seen_foreign_run_queue_generation() -> u64 {
    FOREIGN_RUN_QUEUE_SEEN.with(|seen| *seen.borrow())
}

fn remove_tasklet_object_by_core_id(queue: &mut ThreadRunQueue, core_id: CoreTaskletId) {
    if let Some(index) = queue
        .queue
        .iter()
        .position(|entry| entry.core_id == core_id)
    {
        queue.queue.remove(index);
        queue.queued_ids.remove(&core_id);
    }
}

fn prune_pending_run_queue_removals_for_current() {
    let thread_id = current_thread_id();
    let mut removals = PENDING_RUN_QUEUE_REMOVALS
        .lock()
        .expect("pending run queue removal lock poisoned");
    if !removals
        .iter()
        .any(|removal| removal.thread_id == thread_id)
    {
        return;
    }

    let mut current_len = None;
    let _ = CURRENT_THREAD_RUN_QUEUE.try_with(|cell| {
        let mut queue = cell.borrow_mut();
        if let Some(queue) = queue.as_mut() {
            for removal in removals
                .iter()
                .filter(|removal| removal.thread_id == thread_id)
            {
                remove_tasklet_object_by_core_id(queue, removal.core_id);
            }
            current_len = Some(queue.queue.len());
        }
    });
    removals.retain(|removal| removal.thread_id != thread_id);
    drop(removals);

    if let Some(count) = current_len {
        publish_run_queue_count_change(thread_id, count);
    }
}

fn ensure_global_run_queue_entry_locked(
    queues: &mut Vec<ThreadRunQueue>,
    thread_id: ThreadId,
) -> CoreRunQueueId {
    if let Some(index) = thread_run_queue_index(queues, thread_id) {
        return queues[index].core_queue_id;
    }
    let core_queue_id = bridge_core_create_run_queue();
    queues.push(ThreadRunQueue {
        thread_id,
        core_queue_id,
        queue: VecDeque::new(),
        queued_ids: HashSet::new(),
    });
    core_queue_id
}

fn ensure_current_thread_run_queue(create: bool) -> Option<CoreRunQueueId> {
    if let Some(core_queue_id) = CURRENT_THREAD_RUN_QUEUE
        .with(|cell| cell.borrow().as_ref().map(|queue| queue.core_queue_id))
    {
        return Some(core_queue_id);
    }

    let thread_id = current_thread_id();
    let mut queues = THREAD_RUN_QUEUES
        .lock()
        .expect("thread run queue lock poisoned");
    if let Some(index) = thread_run_queue_index(&queues, thread_id) {
        let entry = &mut queues[index];
        let core_queue_id = entry.core_queue_id;
        let queue = ThreadRunQueue {
            thread_id,
            core_queue_id,
            queue: std::mem::take(&mut entry.queue),
            queued_ids: std::mem::take(&mut entry.queued_ids),
        };
        let count = queue.queue.len();
        CURRENT_THREAD_RUN_QUEUE.with(|cell| {
            *cell.borrow_mut() = Some(queue);
        });
        let generation = RUN_QUEUE_GENERATION.load(Ordering::Acquire);
        set_cached_run_queue_count(generation, count);
        set_seen_foreign_run_queue_generation(FOREIGN_RUN_QUEUE_GENERATION.load(Ordering::Acquire));
        return Some(core_queue_id);
    }

    if !create {
        return None;
    }

    let core_queue_id = ensure_global_run_queue_entry_locked(&mut queues, thread_id);
    drop(queues);
    CURRENT_THREAD_RUN_QUEUE.with(|cell| {
        *cell.borrow_mut() = Some(ThreadRunQueue {
            thread_id,
            core_queue_id,
            queue: VecDeque::new(),
            queued_ids: HashSet::new(),
        });
    });
    let generation = RUN_QUEUE_GENERATION.load(Ordering::Acquire);
    set_cached_run_queue_count(generation, 0);
    set_seen_foreign_run_queue_generation(FOREIGN_RUN_QUEUE_GENERATION.load(Ordering::Acquire));
    Some(core_queue_id)
}

fn merge_foreign_run_queue_for_current() {
    let foreign_generation = FOREIGN_RUN_QUEUE_GENERATION.load(Ordering::Acquire);
    if seen_foreign_run_queue_generation() == foreign_generation {
        prune_pending_run_queue_removals_for_current();
        return;
    }

    let thread_id = current_thread_id();
    let mut merged_count = None;
    let Ok(()) = CURRENT_THREAD_RUN_QUEUE.try_with(|cell| {
        let mut current = cell.borrow_mut();
        if current.is_none() {
            drop(current);
            let _ = ensure_current_thread_run_queue(false);
            current = cell.borrow_mut();
        }
        let Some(current) = current.as_mut() else {
            return;
        };

        let mut queues = THREAD_RUN_QUEUES
            .lock()
            .expect("thread run queue lock poisoned");
        if let Some(index) = thread_run_queue_index(&queues, thread_id) {
            let entry = &mut queues[index];
            while let Some(tasklet) = entry.queue.pop_front() {
                if current.queued_ids.insert(tasklet.core_id) {
                    current.queue.push_back(tasklet);
                }
            }
            entry.queued_ids.clear();
        }
        merged_count = Some(current.queue.len());
    }) else {
        return;
    };
    set_seen_foreign_run_queue_generation(foreign_generation);
    prune_pending_run_queue_removals_for_current();
    if let Some(count) = merged_count {
        let generation = RUN_QUEUE_GENERATION.load(Ordering::Acquire);
        set_cached_run_queue_count(generation, count);
    }
}

fn current_thread_core_run_queue_id(create: bool) -> Option<CoreRunQueueId> {
    ensure_current_thread_run_queue(create)
}

fn pop_tasklet_object_by_core_id(
    queue: &mut ThreadRunQueue,
    core_id: CoreTaskletId,
) -> Option<PyObject> {
    let tasklet = if queue
        .queue
        .front()
        .is_some_and(|entry| entry.core_id == core_id)
    {
        queue.queue.pop_front()
    } else {
        queue
            .queue
            .iter()
            .position(|entry| entry.core_id == core_id)
            .and_then(|index| queue.queue.remove(index))
    }?;
    queue.queued_ids.remove(&core_id);
    Some(tasklet.object)
}

fn remove_tasklet_object_by_ptr(queue: &mut ThreadRunQueue, target: *mut ffi::PyObject) {
    let mut retained = VecDeque::with_capacity(queue.queue.len());
    while let Some(tasklet) = queue.queue.pop_front() {
        if tasklet.object.as_ptr() == target {
            queue.queued_ids.remove(&tasklet.core_id);
        } else {
            retained.push_back(tasklet);
        }
    }
    queue.queue = retained;
}

fn move_or_push_queued_tasklet(
    queue: &mut ThreadRunQueue,
    core_id: CoreTaskletId,
    tasklet_object: PyObject,
) {
    if queue.queued_ids.insert(core_id) {
        queue.queue.push_back(QueuedTasklet {
            core_id,
            object: tasklet_object,
        });
        return;
    }

    if let Some(index) = queue
        .queue
        .iter()
        .position(|entry| entry.core_id == core_id)
    {
        let mut tasklet = queue
            .queue
            .remove(index)
            .expect("queued tasklet index should be valid");
        tasklet.object = tasklet_object;
        queue.queue.push_back(tasklet);
    } else {
        queue.queue.push_back(QueuedTasklet {
            core_id,
            object: tasklet_object,
        });
    }
}

fn queue_tasklet_for_thread(
    py: Python<'_>,
    thread_id: ThreadId,
    tasklet_object: PyObject,
) -> PyResult<()> {
    let core_id = tasklet_core_id(py, &tasklet_object)?;
    queue_tasklet_core_for_thread(py, thread_id, core_id, tasklet_object)
}

fn queue_tasklet_core_for_thread(
    _py: Python<'_>,
    thread_id: ThreadId,
    core_id: CoreTaskletId,
    tasklet_object: PyObject,
) -> PyResult<()> {
    if thread_id == current_thread_id() {
        let core_queue_id = ensure_current_thread_run_queue(true)
            .expect("current thread run queue should be created");
        let resulting_len = CURRENT_THREAD_RUN_QUEUE.with(|cell| {
            let mut queue = cell.borrow_mut();
            let queue = queue
                .as_mut()
                .expect("current thread run queue should exist");
            move_or_push_queued_tasklet(queue, core_id, tasklet_object);
            queue.queue.len()
        });
        bridge_core_schedule_tasklet_back(core_queue_id, core_id)?;
        publish_run_queue_count_change(thread_id, resulting_len);
        return Ok(());
    }

    let resulting_len;
    let core_queue_id;
    let mut queues = THREAD_RUN_QUEUES
        .lock()
        .expect("thread run queue lock poisoned");
    core_queue_id = ensure_global_run_queue_entry_locked(&mut queues, thread_id);
    let index = thread_run_queue_index(&queues, thread_id)
        .expect("global thread run queue entry should exist");
    let entry = &mut queues[index];
    move_or_push_queued_tasklet(entry, core_id, tasklet_object);
    resulting_len = entry.queue.len();
    drop(queues);
    bridge_core_schedule_tasklet_back(core_queue_id, core_id)?;
    publish_foreign_run_queue_change(thread_id, resulting_len);
    Ok(())
}

fn queue_tasklet_for_owner(py: Python<'_>, tasklet_object: &PyObject) -> PyResult<()> {
    let owner_thread = tasklet_owner_thread(py, tasklet_object).unwrap_or_else(current_thread_id);
    queue_tasklet_for_thread(py, owner_thread, tasklet_object.clone_ref(py))
}

fn pop_current_queued_tasklet(_py: Python<'_>) -> PyResult<Option<PyObject>> {
    let thread_id = current_thread_id();
    merge_foreign_run_queue_for_current();
    let Some(core_queue_id) = ensure_current_thread_run_queue(false) else {
        return Ok(None);
    };
    let Some(core_id) = bridge_core_pop_next_runnable_tasklet(core_queue_id)? else {
        let remaining = CURRENT_THREAD_RUN_QUEUE
            .with(|cell| cell.borrow().as_ref().map_or(0, |queue| queue.queue.len()));
        publish_run_queue_count_change(thread_id, remaining);
        return Ok(None);
    };
    let (tasklet, remaining) = CURRENT_THREAD_RUN_QUEUE.with(|cell| {
        let mut queue = cell.borrow_mut();
        let queue = queue
            .as_mut()
            .expect("current thread run queue should exist");
        let tasklet = pop_tasklet_object_by_core_id(queue, core_id).ok_or_else(|| {
            PyRuntimeError::new_err("scheduler core run queue selected a missing Python tasklet")
        })?;
        Ok::<_, PyErr>((tasklet, queue.queue.len()))
    })?;
    publish_run_queue_count_change(thread_id, remaining);
    Ok(Some(tasklet))
}

fn queued_tasklet_count() -> usize {
    let generation = RUN_QUEUE_GENERATION.load(Ordering::Acquire);
    if let Some(count) = cached_run_queue_count(generation) {
        return count;
    }
    merge_foreign_run_queue_for_current();
    let generation = RUN_QUEUE_GENERATION.load(Ordering::Acquire);
    let count = CURRENT_THREAD_RUN_QUEUE
        .with(|cell| cell.borrow().as_ref().map_or(0, |queue| queue.queue.len()));
    set_cached_run_queue_count(generation, count);
    count
}

fn executing_tasklet_count() -> usize {
    EXECUTING_TASKLET.with(|tasklet| usize::from(tasklet.borrow().is_some()))
}

fn remove_queued_tasklet_by_core_id(
    owner_thread: ThreadId,
    core_id: CoreTaskletId,
    target: *mut ffi::PyObject,
) {
    let thread_id = current_thread_id();
    bridge_core_remove_runnable_tasklet(core_id);

    let mut current_len = None;
    if owner_thread == thread_id {
        CURRENT_THREAD_RUN_QUEUE.with(|cell| {
            let mut queue = cell.borrow_mut();
            if let Some(queue) = queue.as_mut() {
                remove_tasklet_object_by_ptr(queue, target);
                current_len = Some(queue.queue.len());
            }
        });
    } else {
        PENDING_RUN_QUEUE_REMOVALS
            .lock()
            .expect("pending run queue removal lock poisoned")
            .push(PendingRunQueueRemoval {
                thread_id: owner_thread,
                core_id,
            });
    }

    let mut queues = THREAD_RUN_QUEUES
        .lock()
        .expect("thread run queue lock poisoned");
    for entry in queues.iter_mut() {
        remove_tasklet_object_by_ptr(entry, target);
    }
    let global_current_len = current_queue_len_from_locked(&queues, thread_id);
    drop(queues);
    if owner_thread == thread_id {
        publish_run_queue_count_change(thread_id, current_len.unwrap_or(global_current_len));
    } else {
        publish_foreign_run_queue_change(owner_thread, 0);
    }
}

fn remove_queued_tasklet(py: Python<'_>, target: *mut ffi::PyObject) {
    if let Some(object) = borrowed_any_from_ptr(py, target) {
        let object = object.to_object(py);
        if let Ok(core_id) = tasklet_core_id(py, &object) {
            let owner_thread = tasklet_owner_thread(py, &object).unwrap_or_else(current_thread_id);
            remove_queued_tasklet_by_core_id(owner_thread, core_id, target);
            return;
        }
    }
    let thread_id = current_thread_id();
    let mut current_len = None;
    CURRENT_THREAD_RUN_QUEUE.with(|cell| {
        let mut queue = cell.borrow_mut();
        if let Some(queue) = queue.as_mut() {
            remove_tasklet_object_by_ptr(queue, target);
            current_len = Some(queue.queue.len());
        }
    });
    let mut queues = THREAD_RUN_QUEUES
        .lock()
        .expect("thread run queue lock poisoned");
    for entry in queues.iter_mut() {
        remove_tasklet_object_by_ptr(entry, target);
    }
    let global_current_len = current_queue_len_from_locked(&queues, thread_id);
    drop(queues);
    publish_run_queue_count_change(thread_id, current_len.unwrap_or(global_current_len));
}

fn take_thread_run_queue(_py: Python<'_>, thread_id: ThreadId) -> PyResult<Vec<PyObject>> {
    if thread_id == current_thread_id() {
        merge_foreign_run_queue_for_current();
        let Some(mut queue) = CURRENT_THREAD_RUN_QUEUE
            .try_with(|cell| cell.borrow_mut().take())
            .ok()
            .flatten()
        else {
            publish_run_queue_count_change(thread_id, 0);
            return Ok(Vec::new());
        };
        let core_ids = bridge_core_clear_run_queue(queue.core_queue_id)?;
        let mut tasklets = Vec::new();
        for core_id in core_ids {
            if let Some(tasklet) = pop_tasklet_object_by_core_id(&mut queue, core_id) {
                tasklets.push(tasklet);
            }
        }
        publish_run_queue_count_change(thread_id, 0);
        return Ok(tasklets);
    }

    take_global_thread_run_queue(thread_id)
}

fn take_global_thread_run_queue(thread_id: ThreadId) -> PyResult<Vec<PyObject>> {
    let mut queues = THREAD_RUN_QUEUES
        .lock()
        .expect("thread run queue lock poisoned");
    if let Some(index) = queues.iter().position(|entry| entry.thread_id == thread_id) {
        let mut entry = queues.remove(index);
        let core_ids = bridge_core_clear_run_queue(entry.core_queue_id)?;
        let mut tasklets = Vec::new();
        for core_id in core_ids {
            if let Some(tasklet) = pop_tasklet_object_by_core_id(&mut entry, core_id) {
                tasklets.push(tasklet);
            }
        }
        drop(queues);
        publish_foreign_run_queue_change(thread_id, 0);
        Ok(tasklets)
    } else {
        Ok(Vec::new())
    }
}

fn drain_thread_run_queue(py: Python<'_>, thread_id: ThreadId) -> PyResult<VecDeque<PyObject>> {
    Ok(take_thread_run_queue(py, thread_id)?.into())
}

fn restore_tasklet_queue_for_thread(
    py: Python<'_>,
    thread_id: ThreadId,
    mut tasklets: VecDeque<PyObject>,
) -> PyResult<()> {
    while let Some(tasklet) = tasklets.pop_front() {
        queue_tasklet_for_thread(py, thread_id, tasklet)?;
    }
    Ok(())
}

fn executing_tasklet(py: Python<'_>) -> Option<PyObject> {
    EXECUTING_TASKLET.with(|tasklet| {
        tasklet
            .borrow()
            .as_ref()
            .map(|tasklet| tasklet.clone_ref(py))
    })
}

fn replace_schedule_callback(py: Python<'_>, callback: Option<PyObject>) -> PyObject {
    let callback_present = callback.is_some();
    let mut slot = SCHEDULE_CALLBACK
        .lock()
        .expect("schedule callback lock poisoned");
    let previous = slot.take().unwrap_or_else(|| py.None());
    *slot = callback;
    SCHEDULE_CALLBACK_PRESENT.store(callback_present, Ordering::Release);
    previous
}

fn schedule_callback(py: Python<'_>) -> Option<PyObject> {
    SCHEDULE_CALLBACK
        .lock()
        .expect("schedule callback lock poisoned")
        .as_ref()
        .map(|callback| callback.clone_ref(py))
}

fn call_schedule_callback(py: Python<'_>, previous: &PyObject, next: &PyObject) -> PyResult<()> {
    if SCHEDULE_CALLBACK_PRESENT.load(Ordering::Acquire) {
        if let Some(callback) = schedule_callback(py) {
            callback.call1(py, (previous, next))?;
        }
    }
    if SCHEDULE_FAST_CALLBACK_PRESENT.load(Ordering::Acquire) {
        if let Some(callback) = *SCHEDULE_FAST_CALLBACK
            .lock()
            .expect("schedule fast callback lock poisoned")
        {
            callback(previous.as_ptr(), next.as_ptr());
        }
    }
    Ok(())
}

fn current_tasklet_object(py: Python<'_>) -> PyResult<PyObject> {
    if let Some(tasklet) = executing_tasklet(py) {
        return Ok(tasklet);
    }
    Ok(current_tasklet(py)?.to_object(py))
}

fn cached_greenlet_object(
    py: Python<'_>,
    selector: impl FnOnce(&GreenletThreadCache) -> &PyObject,
) -> PyResult<PyObject> {
    GREENLET_CACHE.with(|cell| {
        if cell.borrow().is_none() {
            *cell.borrow_mut() = Some(GreenletThreadCache::new(py)?);
        }
        let cache = cell.borrow();
        Ok(selector(
            cache
                .as_ref()
                .expect("greenlet thread cache should be initialized"),
        )
        .clone_ref(py))
    })
}

fn greenlet_type_object(py: Python<'_>) -> PyResult<PyObject> {
    cached_greenlet_object(py, |cache| &cache.greenlet_type)
}

fn greenlet_getcurrent_object(py: Python<'_>) -> PyResult<PyObject> {
    cached_greenlet_object(py, |cache| &cache.getcurrent)
}

fn greenlet_exit_object(py: Python<'_>) -> PyResult<PyObject> {
    cached_greenlet_object(py, |cache| &cache.greenlet_exit)
}

fn current_raw_greenlet(py: Python<'_>) -> PyResult<PyObject> {
    greenlet_getcurrent_object(py)?.call0(py)
}

fn greenlet_is_dead(py: Python<'_>, greenlet: &PyObject) -> PyResult<bool> {
    greenlet.bind(py).getattr("dead")?.is_truthy()
}

fn detach_tasklet_greenlet_runner(py: Python<'_>, greenlet: &PyObject) {
    let Ok(run) = greenlet.bind(py).getattr("run") else {
        return;
    };
    let Ok(runner) = run.downcast::<TaskletGreenletRunner>() else {
        return;
    };
    if let Ok(mut runner) = runner.try_borrow_mut() {
        runner.tasklet = None;
    }
}

fn dispose_tasklet_greenlet(py: Python<'_>, greenlet: &PyObject) {
    if !greenlet_is_dead(py, greenlet).unwrap_or(true) {
        if let Ok(greenlet_exit) = greenlet_exit_object(py) {
            if let Ok(throw) = greenlet.bind(py).getattr("throw") {
                let _ = throw.call1((greenlet_exit,));
            }
        }
    }
    detach_tasklet_greenlet_runner(py, greenlet);
}

fn tasklet_has_live_greenlet(py: Python<'_>, tasklet_object: &PyObject) -> bool {
    tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .ok()
        .and_then(|tasklet| {
            tasklet.try_borrow().ok().and_then(|tasklet| {
                tasklet
                    .greenlet
                    .as_ref()
                    .map(|greenlet| greenlet.clone_ref(py))
            })
        })
        .and_then(|greenlet| greenlet_is_dead(py, &greenlet).ok().map(|dead| !dead))
        .unwrap_or(false)
}

fn ensure_tasklet_greenlet(py: Python<'_>, tasklet_object: &PyObject) -> PyResult<PyObject> {
    if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
        if let Ok(tasklet_ref) = tasklet.try_borrow() {
            if let Some(greenlet) = tasklet_ref.greenlet.as_ref() {
                if !greenlet_is_dead(py, greenlet)? {
                    return Ok(greenlet.clone_ref(py));
                }
            }
        }
    }

    let runner = Py::new(
        py,
        TaskletGreenletRunner {
            tasklet: Some(tasklet_object.clone_ref(py)),
        },
    )?
    .to_object(py);
    let greenlet = greenlet_type_object(py)?.call1(py, (runner,))?;
    let tasklet = tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
    tasklet.try_borrow_mut()?.greenlet = Some(greenlet.clone_ref(py));
    Ok(greenlet)
}

fn switch_tasklet_greenlet(
    py: Python<'_>,
    greenlet: &PyObject,
    args: Option<&PyObject>,
    kwargs: Option<&PyObject>,
    pending_exception: Option<TaskletException>,
) -> PyResult<PyObject> {
    let parent = current_raw_greenlet(py)?;
    greenlet.bind(py).setattr("parent", parent)?;
    if let Some(exception) = pending_exception {
        return exception.throw_into_greenlet(py, greenlet);
    }
    let switch = greenlet.bind(py).getattr("switch")?;
    let result = if let Some(args) = args {
        let args = args
            .bind(py)
            .downcast::<PyTuple>()
            .map_err(|_| PyRuntimeError::new_err("tasklet args must be a tuple"))?;
        let kwargs = kwargs
            .map(|kwargs| kwargs.bind(py).downcast::<PyDict>())
            .transpose()
            .map_err(|_| PyRuntimeError::new_err("tasklet kwargs must be a dict"))?;
        switch.call(args, kwargs)?
    } else {
        switch.call0()?
    };
    Ok(result.to_object(py))
}

fn yield_current_greenlet(py: Python<'_>) -> PyResult<bool> {
    let current = current_raw_greenlet(py)?;
    let parent = current.bind(py).getattr("parent")?;
    let Ok(switch) = parent.getattr("switch") else {
        return Ok(false);
    };
    switch.call0()?;
    Ok(true)
}

fn ensure_switch_allowed() -> PyResult<()> {
    let trapped = if let Some(core_queue_id) = current_thread_core_run_queue_id(false) {
        bridge_core_is_switch_trapped(core_queue_id)?
    } else {
        false
    };
    if trapped {
        Err(PyRuntimeError::new_err("switch_trap"))
    } else {
        Ok(())
    }
}

fn current_tasklet_block_trap(py: Python<'_>) -> PyResult<bool> {
    let current = current_tasklet_object(py)?;
    let current = current
        .bind(py)
        .downcast::<Tasklet>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?
        .try_borrow()?;
    Ok(current.block_trap)
}

fn set_tasklet_paused(py: Python<'_>, tasklet_object: &PyObject, paused: bool) {
    if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
        if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
            tasklet.paused = paused;
            if paused {
                bridge_core_pause_tasklet(tasklet.core_id);
            } else {
                bridge_core_resume_tasklet(tasklet.core_id);
            }
            bridge_core_set_tasklet_block_trap(tasklet.core_id, tasklet.block_trap);
        }
    }
}

fn mark_tasklet_blocked(
    py: Python<'_>,
    tasklet_object: &PyObject,
    channel: Option<PyObject>,
    direction: Option<ChannelBlockDirection>,
) {
    if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
        if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
            tasklet.alive = true;
            tasklet.blocked = true;
            tasklet.scheduled = false;
            tasklet.paused = false;
            tasklet.continuation_pending = false;
            tasklet.kill_pending = false;
            tasklet.pending_exception = None;
            tasklet.blocked_channel = channel;
            tasklet.blocked_direction = direction;
            tasklet.skip_next_channel_callback = false;
        }
    }
}

fn prepare_tasklet_for_channel_continuation(
    py: Python<'_>,
    tasklet_object: &PyObject,
    scheduled: bool,
) {
    if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
        if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
            tasklet.alive = true;
            tasklet.blocked = false;
            tasklet.scheduled = scheduled;
            tasklet.paused = !scheduled;
            tasklet.continuation_pending = true;
            tasklet.kill_pending = false;
            tasklet.pending_exception = None;
            tasklet.blocked_channel = None;
            tasklet.blocked_direction = None;
            tasklet.skip_next_channel_callback = false;
        }
    }
}

fn schedule_tasklet_for_channel_continuation(
    py: Python<'_>,
    tasklet_object: &PyObject,
) -> PyResult<()> {
    prepare_tasklet_for_channel_continuation(py, tasklet_object, true);
    queue_tasklet_for_owner(py, tasklet_object)
}

fn prepare_tasklet_for_channel_replay(py: Python<'_>, tasklet_object: &PyObject, scheduled: bool) {
    if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
        if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
            tasklet.args = tasklet.last_args.as_ref().map(|args| args.clone_ref(py));
            tasklet.kwargs = tasklet
                .last_kwargs
                .as_ref()
                .map(|kwargs| kwargs.clone_ref(py));
            tasklet.alive = true;
            tasklet.blocked = false;
            tasklet.scheduled = scheduled;
            tasklet.paused = false;
            tasklet.continuation_pending = false;
            tasklet.kill_pending = false;
            tasklet.pending_exception = None;
            tasklet.blocked_channel = None;
            tasklet.blocked_direction = None;
            tasklet.skip_next_channel_callback = true;
        }
    }
}

fn schedule_tasklet_for_channel_replay(py: Python<'_>, tasklet_object: &PyObject) -> PyResult<()> {
    prepare_tasklet_for_channel_replay(py, tasklet_object, true);
    queue_tasklet_for_owner(py, tasklet_object)
}

fn continue_channel_tasklet_after_transfer(
    py: Python<'_>,
    tasklet_object: &PyObject,
    immediate: bool,
) -> PyResult<()> {
    let immediate = immediate && tasklet_belongs_to_thread(py, tasklet_object, current_thread_id());
    let has_live_greenlet = tasklet_has_live_greenlet(py, tasklet_object);
    if immediate {
        let yielded_tasklet = queue_current_tasklet_after_channel_handoff(py)?;
        if has_live_greenlet {
            prepare_tasklet_for_channel_continuation(py, tasklet_object, false);
        } else {
            prepare_tasklet_for_channel_replay(py, tasklet_object, false);
        }
        execute_tasklet_object(py, tasklet_object)?;
        if yielded_tasklet {
            yield_current_greenlet(py)?;
        }
        Ok(())
    } else {
        if has_live_greenlet {
            schedule_tasklet_for_channel_continuation(py, tasklet_object)?;
        } else {
            schedule_tasklet_for_channel_replay(py, tasklet_object)?;
        }
        Ok(())
    }
}

fn queue_current_tasklet_after_channel_handoff(py: Python<'_>) -> PyResult<bool> {
    let Some(current) = executing_tasklet(py) else {
        return Ok(false);
    };

    let tasklet = current
        .bind(py)
        .downcast::<Tasklet>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
    {
        let mut tasklet = tasklet.try_borrow_mut()?;
        tasklet.alive = true;
        tasklet.blocked = false;
        tasklet.scheduled = true;
        tasklet.paused = false;
        tasklet.continuation_pending = true;
        tasklet.kill_pending = false;
        tasklet.pending_exception = None;
        tasklet.blocked_channel = None;
        tasklet.blocked_direction = None;
    }
    queue_tasklet_for_owner(py, &current)?;
    Ok(true)
}

fn mark_tasklet_paused(py: Python<'_>, tasklet_object: &PyObject) {
    if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
        if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
            tasklet.alive = true;
            tasklet.blocked = false;
            tasklet.scheduled = false;
            tasklet.paused = true;
            tasklet.continuation_pending = true;
            tasklet.kill_pending = false;
            tasklet.pending_exception = None;
            tasklet.blocked_channel = None;
            tasklet.blocked_direction = None;
            tasklet.skip_next_channel_callback = false;
            tasklet.sync_core_state();
        }
    }
}

fn mark_tasklet_cleared_from_channel(py: Python<'_>, tasklet_object: &PyObject) {
    if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
        if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
            tasklet.args = None;
            tasklet.kwargs = None;
            tasklet.last_args = None;
            tasklet.last_kwargs = None;
            tasklet.alive = false;
            tasklet.blocked = false;
            tasklet.scheduled = false;
            tasklet.paused = false;
            tasklet.continuation_pending = false;
            tasklet.kill_pending = false;
            tasklet.greenlet = None;
            tasklet.pending_exception = None;
            tasklet.blocked_channel = None;
            tasklet.blocked_direction = None;
            tasklet.skip_next_channel_callback = false;
            tasklet.sync_core_state();
        }
    }
}

fn deliver_tasklet_exit_to_blocked_tasklet(py: Python<'_>, tasklet_object: &PyObject) {
    let greenlet = tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .ok()
        .and_then(|tasklet| {
            tasklet.try_borrow().ok().and_then(|tasklet| {
                tasklet
                    .greenlet
                    .as_ref()
                    .map(|greenlet| greenlet.clone_ref(py))
            })
        });

    if let Some(greenlet) = greenlet {
        if greenlet_is_dead(py, &greenlet).is_ok_and(|dead| !dead) {
            if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
                if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
                    tasklet.alive = true;
                    tasklet.blocked = false;
                    tasklet.scheduled = false;
                    tasklet.paused = false;
                    tasklet.continuation_pending = false;
                    tasklet.kill_pending = false;
                    tasklet.pending_exception = None;
                    tasklet.blocked_channel = None;
                    tasklet.blocked_direction = None;
                    tasklet.sync_core_state();
                }
            }

            let previous_executing =
                EXECUTING_TASKLET.with(|cell| cell.replace(Some(tasklet_object.clone_ref(py))));
            let exception =
                TaskletException::set(py.get_type_bound::<TaskletExit>().to_object(py), py.None());
            let result = switch_tasklet_greenlet(py, &greenlet, None, None, Some(exception));
            EXECUTING_TASKLET.with(|cell| {
                cell.replace(previous_executing);
            });
            if let Err(error) = result {
                if !error.is_instance_of::<TaskletExit>(py) {
                    unsafe {
                        ffi::PyErr_Clear();
                    }
                }
            }
        }
    }

    mark_tasklet_cleared_from_channel(py, tasklet_object);
}

fn finish_tasklet_for_thread_exit(py: Python<'_>, tasklet_object: &PyObject) {
    deliver_tasklet_exit_to_blocked_tasklet(py, tasklet_object);
}

fn cleanup_thread_channels(py: Python<'_>, thread_id: ThreadId) {
    let active_channels = ACTIVE_CHANNEL_OBJECTS
        .lock()
        .expect("active channel registry lock poisoned")
        .clone();

    for object_ptr in active_channels {
        let Some(channel_object) = borrowed_any_from_ptr(py, object_ptr as *mut ffi::PyObject)
        else {
            continue;
        };
        let Ok(channel) = channel_object.downcast::<Channel>() else {
            continue;
        };
        let removed = {
            let Ok(mut channel) = channel.try_borrow_mut() else {
                continue;
            };
            if channel.initialized {
                channel.remove_blocked_tasklets_for_thread(py, thread_id)
            } else {
                Vec::new()
            }
        };
        for tasklet in removed {
            finish_tasklet_for_thread_exit(py, &tasklet);
        }
    }
}

fn run_until_channel_progress_or_deadlock(py: Python<'_>) -> PyResult<()> {
    let queued = queued_tasklet_count();
    if queued > 0 {
        run_queued_tasklets(py, queued)?;
    }
    Ok(())
}

fn enqueue_tasklet_object(
    py: Python<'_>,
    tasklet_object: PyObject,
    args: PyObject,
    kwargs: Option<PyObject>,
) -> PyResult<()> {
    {
        let tasklet = tasklet_object
            .bind(py)
            .downcast::<Tasklet>()
            .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
        let mut tasklet = tasklet.try_borrow_mut()?;
        if !tasklet.belongs_to_current_thread() {
            return Err(PyRuntimeError::new_err(
                "Failed to setup tasklet: Cannot setup tasklet from another thread",
            ));
        }
        if tasklet.scheduled {
            return Err(PyRuntimeError::new_err("tasklet is already scheduled"));
        }
        if tasklet.callable.is_none() {
            return Err(PyRuntimeError::new_err("tasklet has no callable"));
        }

        tasklet.args = Some(args);
        tasklet.kwargs = kwargs;
        tasklet.greenlet = None;
        tasklet.scheduled = true;
        tasklet.alive = true;
        tasklet.blocked = false;
        tasklet.paused = false;
        tasklet.continuation_pending = false;
        tasklet.kill_pending = false;
        tasklet.pending_exception = None;
        tasklet.blocked_channel = None;
        tasklet.blocked_direction = None;
        tasklet.sync_core_state();
    }

    queue_tasklet_for_owner(py, &tasklet_object)?;
    Ok(())
}

fn insert_tasklet_object(py: Python<'_>, tasklet_object: PyObject) -> PyResult<()> {
    ensure_switch_allowed()?;
    let should_queue = {
        let tasklet = tasklet_object
            .bind(py)
            .downcast::<Tasklet>()
            .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
        let mut tasklet = tasklet.try_borrow_mut()?;
        if tasklet.is_main {
            return Ok(());
        }
        if tasklet.blocked {
            return Err(PyRuntimeError::new_err(
                "Failed to insert tasklet: Cannot insert blocked tasklet",
            ));
        }
        if !tasklet.alive {
            return Err(PyRuntimeError::new_err(
                "Failed to insert tasklet: Cannot insert dead tasklet",
            ));
        }
        if tasklet.callable.is_none() {
            return Err(PyRuntimeError::new_err("tasklet has no callable"));
        }
        if tasklet.scheduled {
            false
        } else {
            tasklet.paused = false;
            tasklet.scheduled = true;
            bridge_core_resume_tasklet(tasklet.core_id);
            bridge_core_set_tasklet_block_trap(tasklet.core_id, tasklet.block_trap);
            tasklet.sync_core_state();
            true
        }
    };

    if should_queue {
        queue_tasklet_for_owner(py, &tasklet_object)?;
    }
    Ok(())
}

fn kill_tasklet_object(py: Python<'_>, tasklet_object: PyObject, pending: bool) -> PyResult<()> {
    let tasklet = tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
    let tasklet_ptr = tasklet.as_ptr();
    {
        let tasklet_ref = tasklet.try_borrow()?;
        if !tasklet_ref.belongs_to_current_thread() {
            return Err(PyRuntimeError::new_err(
                "Failed to kill tasklet: Cannot kill tasklet from another thread",
            ));
        }
        if !tasklet_ref.alive {
            drop(tasklet_ref);
            remove_queued_tasklet(py, tasklet_ptr);
            return Ok(());
        }
    }

    if executing_tasklet(py).is_some_and(|current| current.as_ptr() == tasklet_ptr) {
        remove_queued_tasklet(py, tasklet_ptr);
        return Err(TaskletExit::new_err("tasklet killed"));
    }

    let exception =
        TaskletException::set(py.get_type_bound::<TaskletExit>().to_object(py), py.None());
    throw_exception_into_tasklet(py, tasklet_object, exception, pending)
}

struct TimeoutRunOutcome {
    completed: bool,
    switched: i32,
}

fn tasklet_times_switched_to(py: Python<'_>, tasklet_object: &PyObject) -> Option<u64> {
    tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .ok()
        .and_then(|tasklet| {
            tasklet
                .try_borrow()
                .ok()
                .map(|tasklet| tasklet.times_switched_to)
        })
}

fn tasklet_completed_for_timeout(py: Python<'_>, tasklet_object: &PyObject) -> bool {
    tasklet_object
        .bind(py)
        .downcast::<Tasklet>()
        .ok()
        .and_then(|tasklet| {
            tasklet.try_borrow().ok().map(|tasklet| {
                !tasklet.alive && !tasklet.blocked && !tasklet.scheduled && !tasklet.paused
            })
        })
        .unwrap_or(false)
}

fn execute_tasklet_object_for_timeout(
    py: Python<'_>,
    tasklet_object: &PyObject,
) -> PyResult<TimeoutRunOutcome> {
    let before_switches = tasklet_times_switched_to(py, tasklet_object).unwrap_or(0);
    let result = execute_tasklet_object(py, tasklet_object);
    let after_switches = tasklet_times_switched_to(py, tasklet_object).unwrap_or(before_switches);
    let switched = if after_switches > before_switches {
        2
    } else {
        0
    };
    let completed = switched > 0 && tasklet_completed_for_timeout(py, tasklet_object);
    result.map(|()| TimeoutRunOutcome {
        completed,
        switched,
    })
}

fn store_timeout_counts(completed: i32, switched: i32) {
    LAST_TIMEOUT_COMPLETED_TASKLETS.store(completed, Ordering::SeqCst);
    LAST_TIMEOUT_SWITCHED_TASKLETS.store(switched, Ordering::SeqCst);
}

fn run_with_timeout(py: Python<'_>, timeout: i64) -> PyResult<()> {
    ensure_switch_allowed()?;
    store_timeout_counts(0, 0);

    let timeout_nanos = u128::try_from(timeout)
        .ok()
        .filter(|timeout| *timeout > 0)
        .map(|timeout| timeout.max(MIN_POSITIVE_RUN_TIMEOUT_NANOS))
        .unwrap_or(0);
    let start = Instant::now();
    let mut first_time_limit_test_skipped = false;
    let mut completed = 0;
    let mut switched = 0;

    while queued_tasklet_count() > 0 {
        if start.elapsed().as_nanos() >= timeout_nanos {
            if first_time_limit_test_skipped {
                break;
            }
            first_time_limit_test_skipped = true;
        }

        let Some(tasklet_object) = pop_current_queued_tasklet(py)? else {
            break;
        };
        match execute_tasklet_object_for_timeout(py, &tasklet_object) {
            Ok(outcome) => {
                if outcome.completed {
                    completed += 1;
                }
                switched += outcome.switched;
                store_timeout_counts(completed, switched);
            }
            Err(error) => {
                store_timeout_counts(completed, switched);
                return Err(error);
            }
        }
    }

    Ok(())
}

fn run_tasklet_by_ptr(py: Python<'_>, target: *mut ffi::PyObject) -> PyResult<()> {
    let nested = get_use_nested_tasklets();
    let mut skipped = VecDeque::new();
    let mut selected = VecDeque::new();
    let mut found = false;

    let thread_id = current_thread_id();
    let mut queued = drain_thread_run_queue(py, thread_id)?;

    while let Some(tasklet) = queued.pop_front() {
        if tasklet.as_ptr() == target {
            found = true;
            selected.push_back(tasklet);
            if nested {
                while let Some(tasklet) = queued.pop_front() {
                    selected.push_back(tasklet);
                }
                restore_tasklet_queue_for_thread(py, thread_id, std::mem::take(&mut skipped))?;
            } else {
                while let Some(tasklet) = skipped.pop_front() {
                    selected.push_back(tasklet);
                }
                while let Some(tasklet) = queued.pop_front() {
                    selected.push_back(tasklet);
                }
            }
            break;
        }
        skipped.push_back(tasklet);
    }

    if !found {
        while let Some(tasklet) = queued.pop_front() {
            skipped.push_back(tasklet);
        }
        if !skipped.is_empty() {
            restore_tasklet_queue_for_thread(py, thread_id, skipped)?;
        }
    }

    if !found {
        let tasklet_object = borrowed_any_from_ptr(py, target)
            .ok_or_else(|| PyRuntimeError::new_err("tasklet is not scheduled"))?
            .to_object(py);
        let can_run_paused = tasklet_object
            .bind(py)
            .downcast::<Tasklet>()
            .ok()
            .and_then(|tasklet| {
                tasklet.try_borrow().ok().map(|tasklet| {
                    let core_paused = tasklet
                        .core_snapshot()
                        .ok()
                        .is_some_and(|snapshot| snapshot.paused && !snapshot.scheduled);
                    core_paused
                        && !tasklet.blocked
                        && !tasklet.scheduled
                        && tasklet.callable.is_some()
                        && (tasklet.args.is_some()
                            || tasklet.continuation_pending
                            || tasklet.pending_exception.is_some())
                })
            })
            .unwrap_or(false);
        if !can_run_paused {
            return Err(PyRuntimeError::new_err("tasklet is not scheduled"));
        }
        execute_tasklet_object(py, &tasklet_object)?;
        return Ok(());
    }

    run_tasklet_objects(py, selected)?;
    Ok(())
}

fn run_queued_tasklets(py: Python<'_>, number_of_tasklets: usize) -> PyResult<usize> {
    if number_of_tasklets == 0 {
        return Err(PyRuntimeError::new_err(
            "Invalid number: Number of Tasklets to run must be greater than 0.",
        ));
    }

    let mut ran = 0;
    for _ in 0..number_of_tasklets {
        let Some(tasklet_object) = pop_current_queued_tasklet(py)? else {
            break;
        };

        execute_tasklet_object(py, &tasklet_object)?;
        ran += 1;
    }

    Ok(ran)
}

fn run_tasklet_objects(py: Python<'_>, mut tasklets: VecDeque<PyObject>) -> PyResult<usize> {
    let mut ran = 0;
    while let Some(tasklet_object) = tasklets.pop_front() {
        execute_tasklet_object(py, &tasklet_object)?;
        ran += 1;
    }
    Ok(ran)
}

fn execute_tasklet_object(py: Python<'_>, tasklet_object: &PyObject) -> PyResult<()> {
    let (args, kwargs, pending_exception) = {
        let tasklet = tasklet_object
            .bind(py)
            .downcast::<Tasklet>()
            .map_err(|_| PyTypeError::new_err("expected scheduler.tasklet"))?;
        let mut tasklet = tasklet.try_borrow_mut()?;
        if tasklet.kill_pending {
            tasklet.args = None;
            tasklet.kwargs = None;
            tasklet.greenlet = None;
            tasklet.alive = false;
            tasklet.blocked = false;
            tasklet.scheduled = false;
            tasklet.paused = false;
            tasklet.continuation_pending = false;
            tasklet.kill_pending = false;
            tasklet.sync_core_state();
            return Ok(());
        }
        let has_live_greenlet = tasklet
            .greenlet
            .as_ref()
            .map(|greenlet| greenlet_is_dead(py, greenlet).map(|dead| !dead))
            .transpose()?
            .unwrap_or(false);
        if tasklet.continuation_pending && !has_live_greenlet {
            tasklet.args = None;
            tasklet.kwargs = None;
            tasklet.greenlet = None;
            tasklet.alive = false;
            tasklet.blocked = false;
            tasklet.scheduled = false;
            tasklet.paused = false;
            tasklet.continuation_pending = false;
            tasklet.kill_pending = false;
            tasklet.sync_core_state();
            return Ok(());
        }
        let pending_exception = tasklet.pending_exception.take();
        if let Some(exception) = pending_exception {
            if has_live_greenlet {
                tasklet.pending_exception = Some(exception);
            } else {
                let is_tasklet_exit = exception.is_tasklet_exit(py);
                tasklet.args = None;
                tasklet.kwargs = None;
                tasklet.greenlet = None;
                tasklet.alive = false;
                tasklet.blocked = false;
                tasklet.scheduled = false;
                tasklet.paused = false;
                tasklet.continuation_pending = false;
                tasklet.kill_pending = false;
                tasklet.blocked_channel = None;
                tasklet.blocked_direction = None;
                tasklet.sync_core_state();
                if is_tasklet_exit {
                    return Ok(());
                }
                return Err(exception.raise(py));
            }
        }

        tasklet.scheduled = false;
        tasklet.blocked = false;
        tasklet.paused = false;
        tasklet.continuation_pending = false;
        tasklet.kill_pending = false;
        tasklet.blocked_channel = None;
        tasklet.blocked_direction = None;
        tasklet.times_switched_to += 1;
        tasklet.start_time = monotonic_count();
        tasklet.end_time = 0;
        tasklet.run_time = 0.0;
        tasklet.sync_core_state();
        if tasklet.callable.is_none() {
            return Err(PyRuntimeError::new_err("tasklet has no callable"));
        }

        if has_live_greenlet {
            (None, None, tasklet.pending_exception.take())
        } else {
            let args = tasklet
                .args
                .take()
                .unwrap_or_else(|| PyTuple::empty_bound(py).to_object(py));
            let kwargs = tasklet.kwargs.take();
            tasklet.last_args = Some(args.clone_ref(py));
            tasklet.last_kwargs = kwargs.as_ref().map(|kwargs| kwargs.clone_ref(py));
            (Some(args), kwargs, tasklet.pending_exception.take())
        }
    };

    let previous_current = if let Some(tasklet) = executing_tasklet(py) {
        tasklet
    } else {
        current_tasklet(py)?.to_object(py)
    };
    call_schedule_callback(py, &previous_current, tasklet_object)?;

    let greenlet = ensure_tasklet_greenlet(py, tasklet_object)?;
    let previous_executing =
        EXECUTING_TASKLET.with(|cell| cell.replace(Some(tasklet_object.clone_ref(py))));
    let switch_result = switch_tasklet_greenlet(
        py,
        &greenlet,
        args.as_ref(),
        kwargs.as_ref(),
        pending_exception,
    );
    EXECUTING_TASKLET.with(|cell| {
        cell.replace(previous_executing);
    });
    let end_time = monotonic_count();
    let callback_result = call_schedule_callback(py, tasklet_object, &previous_current);

    if let Err(error) = switch_result {
        mark_tasklet_finished_at(py, tasklet_object, end_time);
        if error.is_instance_of::<TaskletExit>(py) {
            callback_result?;
            return Ok(());
        }
        return Err(error);
    }

    if greenlet_is_dead(py, &greenlet)? {
        mark_tasklet_finished_at(py, tasklet_object, end_time);
    }

    callback_result?;
    Ok(())
}

fn call_context_exit(py: Python<'_>, exit_callable: &PyObject) -> PyResult<()> {
    exit_callable.call1(py, (py.None(), py.None(), py.None()))?;
    Ok(())
}

fn call_exception_handler(
    py: Python<'_>,
    exception_handler: &PyObject,
    error: &PyErr,
    context: &str,
) {
    let info = format!(
        "Unhandled exception in <Tasklet alive=1 blocked=0 paused=0 scheduled=0 context={context}>"
    );
    let exc_type = error.get_type_bound(py).to_object(py);
    let exc_value = error.value_bound(py).to_object(py);
    let exc_traceback = error
        .traceback_bound(py)
        .map(|traceback| traceback.to_object(py));

    unsafe {
        ffi::PyErr_SetExcInfo(
            exc_type.into_ptr(),
            exc_value.into_ptr(),
            exc_traceback.map_or(ptr::null_mut(), PyObject::into_ptr),
        );
    }
    let _ = exception_handler.call1(py, (info,));
    unsafe {
        ffi::PyErr_SetExcInfo(ptr::null_mut(), ptr::null_mut(), ptr::null_mut());
    }
}

fn mark_tasklet_finished_at(py: Python<'_>, tasklet_object: &PyObject, end_time: i64) {
    if let Ok(tasklet) = tasklet_object.bind(py).downcast::<Tasklet>() {
        if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
            if tasklet.start_time > 0 {
                tasklet.end_time = end_time.max(tasklet.start_time + 1);
                tasklet.run_time = (tasklet.end_time - tasklet.start_time) as f64;
            }
            tasklet.alive = false;
            tasklet.blocked = false;
            tasklet.scheduled = false;
            tasklet.paused = false;
            tasklet.continuation_pending = false;
            tasklet.kill_pending = false;
            tasklet.pending_exception = None;
            tasklet.blocked_channel = None;
            tasklet.blocked_direction = None;
            tasklet.sync_core_state();
        }
    }
}

fn monotonic_count() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

extern "C" fn c_api_py_tasklet_new(
    tasklet_type: *mut ffi::PyTypeObject,
    args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    Python::with_gil(|py| unsafe {
        let type_object = if tasklet_type.is_null() {
            py.get_type_bound::<Tasklet>().as_ptr()
        } else {
            tasklet_type.cast::<ffi::PyObject>()
        };

        ffi::PyObject_CallObject(type_object, args)
    })
}

extern "C" fn c_api_py_tasklet_setup(
    tasklet: *mut ffi::PyObject,
    args: *mut ffi::PyObject,
    kwargs: *mut ffi::PyObject,
) -> i32 {
    Python::with_gil(|py| {
        if tasklet.is_null() {
            PyTypeError::new_err("expected scheduler.tasklet").restore(py);
            return -1;
        }

        let args = if args.is_null() {
            PyTuple::empty_bound(py).to_object(py)
        } else {
            unsafe { PyObject::from_borrowed_ptr(py, args) }
        };
        let kwargs = if kwargs.is_null() {
            None
        } else {
            Some(unsafe { PyObject::from_borrowed_ptr(py, kwargs) })
        };
        let tasklet_object = unsafe { PyObject::from_borrowed_ptr(py, tasklet) };

        match enqueue_tasklet_object(py, tasklet_object, args, kwargs) {
            Ok(()) => 0,
            Err(error) => {
                error.restore(py);
                -1
            }
        }
    })
}

extern "C" fn c_api_py_tasklet_insert(tasklet: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        let Some(tasklet_object) =
            borrowed_any_from_ptr(py, tasklet).map(|object| object.to_object(py))
        else {
            PyTypeError::new_err("expected scheduler.tasklet").restore(py);
            return -1;
        };
        c_int_from_py_result(py, insert_tasklet_object(py, tasklet_object))
    })
}

extern "C" fn c_api_py_tasklet_get_block_trap(tasklet: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        borrowed_any_from_ptr(py, tasklet)
            .and_then(|object| {
                object
                    .downcast::<Tasklet>()
                    .ok()
                    .and_then(|tasklet| tasklet.try_borrow().ok().map(|tasklet| tasklet.block_trap))
            })
            .map_or(0, i32::from)
    })
}

extern "C" fn c_api_py_tasklet_set_block_trap(tasklet: *mut ffi::PyObject, value: i32) {
    Python::with_gil(|py| {
        if let Some(Ok(tasklet)) =
            borrowed_any_from_ptr(py, tasklet).map(|object| object.downcast_into::<Tasklet>())
        {
            if let Ok(mut tasklet) = tasklet.try_borrow_mut() {
                tasklet.block_trap = value != 0;
                tasklet.sync_core_state();
            }
        }
    });
}

extern "C" fn c_api_py_tasklet_is_main(tasklet: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        borrowed_any_from_ptr(py, tasklet)
            .and_then(|object| {
                object
                    .downcast::<Tasklet>()
                    .ok()
                    .and_then(|tasklet| tasklet.try_borrow().ok().map(|tasklet| tasklet.is_main))
            })
            .map_or(0, i32::from)
    })
}

extern "C" fn c_api_py_tasklet_check(object: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        borrowed_any_from_ptr(py, object).is_some_and(|object| object.downcast::<Tasklet>().is_ok())
            as i32
    })
}

extern "C" fn c_api_py_tasklet_alive(tasklet: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        borrowed_any_from_ptr(py, tasklet)
            .and_then(|object| {
                object
                    .downcast::<Tasklet>()
                    .ok()
                    .and_then(|tasklet| tasklet.try_borrow().ok().map(|tasklet| tasklet.alive))
            })
            .map_or(0, i32::from)
    })
}

extern "C" fn c_api_py_tasklet_kill(tasklet: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        let Some(tasklet_object) =
            borrowed_any_from_ptr(py, tasklet).map(|object| object.to_object(py))
        else {
            PyTypeError::new_err("expected scheduler.tasklet").restore(py);
            return -1;
        };
        c_int_from_py_result(py, kill_tasklet_object(py, tasklet_object, false))
    })
}

extern "C" fn c_api_py_channel_new(channel_type: *mut ffi::PyTypeObject) -> *mut ffi::PyObject {
    Python::with_gil(|py| unsafe {
        let type_object = if channel_type.is_null() {
            py.get_type_bound::<Channel>().as_ptr()
        } else {
            channel_type.cast::<ffi::PyObject>()
        };

        ffi::PyObject_CallObject(type_object, ptr::null_mut())
    })
}

extern "C" fn c_api_py_channel_send(channel: *mut ffi::PyObject, value: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        if value.is_null() {
            PyTypeError::new_err("channel send value must not be null").restore(py);
            return -1;
        }
        let channel_object = match channel_object_from_ptr(py, channel) {
            Ok(channel_object) => channel_object,
            Err(error) => {
                error.restore(py);
                return -1;
            }
        };
        let value = unsafe { PyObject::from_borrowed_ptr(py, value) };
        c_int_from_py_result(
            py,
            send_channel_message(py, channel_object, ChannelMessage::Value(value)),
        )
    })
}

extern "C" fn c_api_py_channel_receive(channel: *mut ffi::PyObject) -> *mut ffi::PyObject {
    Python::with_gil(|py| {
        let channel_object = match channel_object_from_ptr(py, channel) {
            Ok(channel_object) => channel_object,
            Err(error) => {
                error.restore(py);
                return ptr::null_mut();
            }
        };
        match receive_channel_message(py, channel_object) {
            Ok(value) => value.into_ptr(),
            Err(error) => {
                error.restore(py);
                ptr::null_mut()
            }
        }
    })
}

extern "C" fn c_api_py_channel_send_exception(
    channel: *mut ffi::PyObject,
    exc: *mut ffi::PyObject,
    value: *mut ffi::PyObject,
) -> i32 {
    Python::with_gil(|py| {
        if exc.is_null() {
            PyRuntimeError::new_err("Exception type or instance required").restore(py);
            return -1;
        }
        let channel_object = match channel_object_from_ptr(py, channel) {
            Ok(channel_object) => channel_object,
            Err(error) => {
                error.restore(py);
                return -1;
            }
        };
        let exc = unsafe { PyObject::from_borrowed_ptr(py, exc) };
        if let Err(error) = validate_exception_type_or_instance(
            py,
            &exc,
            "Exception type or instance required",
            ExceptionValidationError::Runtime,
        ) {
            error.restore(py);
            return -1;
        }
        let value = if value.is_null() {
            py.None()
        } else {
            unsafe { PyObject::from_borrowed_ptr(py, value) }
        };
        c_int_from_py_result(
            py,
            send_channel_message(
                py,
                channel_object,
                ChannelMessage::SetException { exc, value },
            ),
        )
    })
}

extern "C" fn c_api_py_channel_send_throw(
    channel: *mut ffi::PyObject,
    exc: *mut ffi::PyObject,
    value: *mut ffi::PyObject,
    traceback: *mut ffi::PyObject,
) -> i32 {
    Python::with_gil(|py| {
        if exc.is_null() {
            PyRuntimeError::new_err("Exception type or instance required").restore(py);
            return -1;
        }
        let channel_object = match channel_object_from_ptr(py, channel) {
            Ok(channel_object) => channel_object,
            Err(error) => {
                error.restore(py);
                return -1;
            }
        };
        let exc = unsafe { PyObject::from_borrowed_ptr(py, exc) };
        if let Err(error) = validate_exception_type_or_instance(
            py,
            &exc,
            "Exception type or instance required",
            ExceptionValidationError::Runtime,
        ) {
            error.restore(py);
            return -1;
        }
        let value = if value.is_null() {
            py.None()
        } else {
            unsafe { PyObject::from_borrowed_ptr(py, value) }
        };
        let traceback = if traceback.is_null() {
            py.None()
        } else {
            unsafe { PyObject::from_borrowed_ptr(py, traceback) }
        };
        c_int_from_py_result(
            py,
            send_channel_message(
                py,
                channel_object,
                ChannelMessage::RestoreException {
                    exc,
                    value,
                    traceback,
                },
            ),
        )
    })
}

extern "C" fn c_api_py_channel_get_queue(channel: *mut ffi::PyObject) -> *mut ffi::PyObject {
    Python::with_gil(|py| {
        let Some(object) = borrowed_any_from_ptr(py, channel) else {
            return ptr::null_mut();
        };
        let Ok(channel) = object.downcast::<Channel>() else {
            return ptr::null_mut();
        };
        let Ok(channel) = channel.try_borrow() else {
            return ptr::null_mut();
        };
        match channel.queue(py) {
            Ok(queue) => queue.into_ptr(),
            Err(error) => {
                error.restore(py);
                ptr::null_mut()
            }
        }
    })
}

extern "C" fn c_api_py_channel_get_preference(channel: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        borrowed_any_from_ptr(py, channel)
            .and_then(|object| {
                object
                    .downcast::<Channel>()
                    .ok()
                    .and_then(|channel| channel.try_borrow().ok().map(|channel| channel.preference))
            })
            .unwrap_or(0)
    })
}

extern "C" fn c_api_py_channel_set_preference(channel: *mut ffi::PyObject, preference: i32) {
    Python::with_gil(|py| {
        if let Some(Ok(channel)) =
            borrowed_any_from_ptr(py, channel).map(|object| object.downcast_into::<Channel>())
        {
            if let Ok(mut channel) = channel.try_borrow_mut() {
                channel.preference = preference.clamp(-1, 1);
                if let Some(core_id) = channel.core_id {
                    if let Err(error) =
                        bridge_core_set_channel_preference(core_id, channel.preference)
                    {
                        error.restore(py);
                    }
                }
            }
        }
    });
}

extern "C" fn c_api_py_channel_get_balance(channel: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        borrowed_any_from_ptr(py, channel)
            .and_then(|object| {
                object.downcast::<Channel>().ok().and_then(|channel| {
                    channel
                        .try_borrow()
                        .ok()
                        .and_then(|channel| channel.balance().ok())
                })
            })
            .unwrap_or(0)
    })
}

extern "C" fn c_api_py_channel_check(object: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        borrowed_any_from_ptr(py, object).is_some_and(|object| object.downcast::<Channel>().is_ok())
            as i32
    })
}

extern "C" fn c_api_py_scheduler_get_scheduler() -> *mut ffi::PyObject {
    Python::with_gil(|py| match schedule_manager(py) {
        Ok(manager) => manager.into_ptr().cast::<ffi::PyObject>(),
        Err(error) => {
            error.restore(py);
            ptr::null_mut()
        }
    })
}

extern "C" fn c_api_py_scheduler_get_run_count() -> i32 {
    getruncount() as i32
}

extern "C" fn c_api_py_scheduler_get_current() -> *mut ffi::PyObject {
    Python::with_gil(|py| match current_tasklet_object(py) {
        Ok(tasklet) => tasklet.into_ptr().cast::<ffi::PyObject>(),
        Err(error) => {
            error.restore(py);
            ptr::null_mut()
        }
    })
}

extern "C" fn c_api_py_scheduler_schedule(
    _retval: *mut ffi::PyObject,
    remove: i32,
) -> *mut ffi::PyObject {
    Python::with_gil(|py| {
        let result = if remove == 0 {
            schedule(py)
        } else {
            schedule_remove(py)
        };
        match result {
            Ok(()) => py.None().into_ptr(),
            Err(error) => {
                error.restore(py);
                ptr::null_mut()
            }
        }
    })
}

extern "C" fn c_api_py_scheduler_run_with_timeout(timeout: i64) -> *mut ffi::PyObject {
    Python::with_gil(|py| match run_with_timeout(py, timeout) {
        Ok(()) => py.None().into_ptr(),
        Err(error) => {
            error.restore(py);
            ptr::null_mut()
        }
    })
}

extern "C" fn c_api_py_scheduler_run_n_tasklets(number_of_tasklets: i32) -> *mut ffi::PyObject {
    Python::with_gil(|py| {
        if number_of_tasklets <= 0 {
            PyRuntimeError::new_err(
                "Invalid number: Number of Tasklets to run must be greater than 0.",
            )
            .restore(py);
            return ptr::null_mut();
        }

        match run_queued_tasklets(py, number_of_tasklets as usize) {
            Ok(_) => py.None().into_ptr(),
            Err(error) => {
                error.restore(py);
                ptr::null_mut()
            }
        }
    })
}

extern "C" fn c_api_py_scheduler_set_channel_callback(callback: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        let replacement = if callback.is_null() {
            None
        } else {
            let callback = unsafe { PyObject::from_borrowed_ptr(py, callback) };
            match validated_callback_replacement(py, callback) {
                Ok(callback) => callback,
                Err(error) => {
                    error.restore(py);
                    return -1;
                }
            }
        };
        CHANNEL_CALLBACK_TOUCHED.with(|touched| {
            *touched.borrow_mut() = true;
        });
        CHANNEL_CALLBACK.with(|slot| *slot.borrow_mut() = replacement);
        0
    })
}

extern "C" fn c_api_py_scheduler_get_channel_callback() -> *mut ffi::PyObject {
    Python::with_gil(|py| get_channel_callback(py).into_ptr())
}

extern "C" fn c_api_py_scheduler_set_schedule_callback(callback: *mut ffi::PyObject) -> i32 {
    Python::with_gil(|py| {
        let replacement = if callback.is_null() {
            None
        } else {
            let callback = unsafe { PyObject::from_borrowed_ptr(py, callback) };
            match validated_callback_replacement(py, callback) {
                Ok(callback) => callback,
                Err(error) => {
                    error.restore(py);
                    return -1;
                }
            }
        };
        replace_schedule_callback(py, replacement);
        0
    })
}

extern "C" fn c_api_py_scheduler_set_schedule_fast_callback(callback: Option<ScheduleHookFunc>) {
    SCHEDULE_FAST_CALLBACK_PRESENT.store(callback.is_some(), Ordering::Release);
    *SCHEDULE_FAST_CALLBACK
        .lock()
        .expect("schedule fast callback lock poisoned") = callback;
}

extern "C" fn c_api_py_scheduler_get_number_of_active_schedule_managers() -> i32 {
    get_number_of_active_schedule_managers() as i32
}

extern "C" fn c_api_py_scheduler_get_number_of_active_channels() -> i32 {
    get_number_of_active_channels() as i32
}

extern "C" fn c_api_py_scheduler_get_all_time_tasklet_count() -> i32 {
    get_all_time_tasklet_count() as i32
}

extern "C" fn c_api_py_scheduler_get_active_tasklet_count() -> i32 {
    get_active_tasklet_count() as i32
}

extern "C" fn c_api_py_scheduler_get_tasklets_completed_last_run_with_timeout() -> i32 {
    LAST_TIMEOUT_COMPLETED_TASKLETS.load(Ordering::SeqCst)
}

extern "C" fn c_api_py_scheduler_get_tasklets_switched_last_run_with_timeout() -> i32 {
    LAST_TIMEOUT_SWITCHED_TASKLETS.load(Ordering::SeqCst)
}

extern "C" fn c_api_py_tasklet_get_times_switched_to(tasklet: *mut ffi::PyObject) -> c_long {
    Python::with_gil(|py| {
        borrowed_any_from_ptr(py, tasklet)
            .and_then(|object| {
                object.downcast::<Tasklet>().ok().and_then(|tasklet| {
                    tasklet.try_borrow().ok().and_then(|tasklet| {
                        tasklet
                            .core_snapshot()
                            .ok()
                            .map(|snapshot| snapshot.times_switched_to as c_long)
                    })
                })
            })
            .unwrap_or(0)
    })
}

extern "C" fn c_api_py_tasklet_get_context(tasklet: *mut ffi::PyObject) -> *const c_char {
    Python::with_gil(|py| {
        let Some(context) = borrowed_any_from_ptr(py, tasklet).and_then(|object| {
            object.downcast::<Tasklet>().ok().and_then(|tasklet| {
                tasklet
                    .try_borrow()
                    .ok()
                    .map(|tasklet| tasklet.context.clone())
            })
        }) else {
            return ptr::null();
        };

        TASKLET_CONTEXT_C_BUFFER.with(|buffer| {
            let mut buffer = buffer.borrow_mut();
            buffer.clear();
            buffer.extend_from_slice(context.as_bytes());
            buffer.push(0);
            buffer.as_ptr().cast::<c_char>()
        })
    })
}

fn create_scheduler_c_api_capsule(py: Python<'_>) -> PyResult<PyObject> {
    let tasklet_exit = py.get_type_bound::<TaskletExit>();
    unsafe {
        *SCHEDULER_TASKLET_EXIT.as_mut_ptr() = tasklet_exit.as_ptr();
    }
    let api = SchedulerCapsuleApi {
        py_tasklet_new: py_object_py_type_py_object_c_api_ptr(c_api_py_tasklet_new),
        py_tasklet_setup: int_py_object_py_object_py_object_c_api_ptr(c_api_py_tasklet_setup),
        py_tasklet_insert: int_py_object_c_api_ptr(c_api_py_tasklet_insert),
        py_tasklet_get_block_trap: int_py_object_c_api_ptr(c_api_py_tasklet_get_block_trap),
        py_tasklet_set_block_trap: void_py_object_int_c_api_ptr(c_api_py_tasklet_set_block_trap),
        py_tasklet_is_main: int_py_object_c_api_ptr(c_api_py_tasklet_is_main),
        py_tasklet_check: int_py_object_c_api_ptr(c_api_py_tasklet_check),
        py_tasklet_alive: int_py_object_c_api_ptr(c_api_py_tasklet_alive),
        py_tasklet_kill: int_py_object_c_api_ptr(c_api_py_tasklet_kill),
        py_channel_new: py_object_py_type_c_api_ptr(c_api_py_channel_new),
        py_channel_send: int_py_object_py_object_c_api_ptr(c_api_py_channel_send),
        py_channel_receive: py_object_py_object_c_api_ptr(c_api_py_channel_receive),
        py_channel_send_exception: int_py_object_py_object_py_object_c_api_ptr(
            c_api_py_channel_send_exception,
        ),
        py_channel_get_queue: py_object_py_object_c_api_ptr(c_api_py_channel_get_queue),
        py_channel_get_preference: int_py_object_c_api_ptr(c_api_py_channel_get_preference),
        py_channel_set_preference: void_py_object_int_c_api_ptr(c_api_py_channel_set_preference),
        py_channel_get_balance: int_py_object_c_api_ptr(c_api_py_channel_get_balance),
        py_channel_check: int_py_object_c_api_ptr(c_api_py_channel_check),
        py_channel_send_throw: int_py_object_py_object_py_object_py_object_c_api_ptr(
            c_api_py_channel_send_throw,
        ),
        py_scheduler_get_scheduler: py_object_c_api_ptr(c_api_py_scheduler_get_scheduler),
        py_scheduler_schedule: py_object_py_object_int_c_api_ptr(c_api_py_scheduler_schedule),
        py_scheduler_get_run_count: int_c_api_ptr(c_api_py_scheduler_get_run_count),
        py_scheduler_get_current: py_object_c_api_ptr(c_api_py_scheduler_get_current),
        py_scheduler_run_with_timeout: py_object_i64_c_api_ptr(c_api_py_scheduler_run_with_timeout),
        py_scheduler_run_n_tasklets: py_object_int_c_api_ptr(c_api_py_scheduler_run_n_tasklets),
        py_scheduler_set_channel_callback: int_py_object_c_api_ptr(
            c_api_py_scheduler_set_channel_callback,
        ),
        py_scheduler_get_channel_callback: py_object_c_api_ptr(
            c_api_py_scheduler_get_channel_callback,
        ),
        py_scheduler_set_schedule_callback: int_py_object_c_api_ptr(
            c_api_py_scheduler_set_schedule_callback,
        ),
        py_scheduler_set_schedule_fast_callback: void_schedule_hook_c_api_ptr(
            c_api_py_scheduler_set_schedule_fast_callback,
        ),
        py_scheduler_get_number_of_active_schedule_managers: int_c_api_ptr(
            c_api_py_scheduler_get_number_of_active_schedule_managers,
        ),
        py_scheduler_get_number_of_active_channels: int_c_api_ptr(
            c_api_py_scheduler_get_number_of_active_channels,
        ),
        py_scheduler_get_all_time_tasklet_count: int_c_api_ptr(
            c_api_py_scheduler_get_all_time_tasklet_count,
        ),
        py_scheduler_get_active_tasklet_count: int_c_api_ptr(
            c_api_py_scheduler_get_active_tasklet_count,
        ),
        py_scheduler_get_tasklets_completed_last_run_with_timeout: int_c_api_ptr(
            c_api_py_scheduler_get_tasklets_completed_last_run_with_timeout,
        ),
        py_scheduler_get_tasklets_switched_last_run_with_timeout: int_c_api_ptr(
            c_api_py_scheduler_get_tasklets_switched_last_run_with_timeout,
        ),
        py_tasklet_type: py.get_type_bound::<Tasklet>().as_type_ptr(),
        py_channel_type: py.get_type_bound::<Channel>().as_type_ptr(),
        tasklet_exit: SCHEDULER_TASKLET_EXIT.as_mut_ptr(),
        py_tasklet_get_times_switched_to: long_py_object_c_api_ptr(
            c_api_py_tasklet_get_times_switched_to,
        ),
        py_tasklet_get_context: cstr_py_object_c_api_ptr(c_api_py_tasklet_get_context),
    };
    unsafe {
        *SCHEDULER_CAPSULE_API.as_mut_ptr() = api;
    }
    let capsule = unsafe {
        ffi::PyCapsule_New(
            SCHEDULER_CAPSULE_API.as_mut_ptr().cast::<c_void>(),
            scheduler_capsule_name_ptr(),
            None,
        )
    };
    if capsule.is_null() {
        Err(PyErr::fetch(py))
    } else {
        Ok(unsafe { PyObject::from_owned_ptr(py, capsule) })
    }
}

fn current_tasklet(py: Python<'_>) -> PyResult<Py<Tasklet>> {
    ensure_thread_cleanup(py)?;
    CURRENT_TASKLET.with(|cell| {
        let mut tasklet = cell.borrow_mut();
        if tasklet.is_none() {
            *tasklet = Some(Py::new(py, Tasklet::unbound(true))?.into_ptr());
        }

        let ptr = tasklet.expect("tasklet initialized");
        Ok(unsafe { Py::from_borrowed_ptr(py, ptr) })
    })
}

fn schedule_manager(py: Python<'_>) -> PyResult<Py<ScheduleManager>> {
    ensure_thread_cleanup(py)?;
    SCHEDULE_MANAGER.with(|cell| {
        let mut manager = cell.borrow_mut();
        if manager.is_none() {
            *manager = Some(ThreadScheduleManager::new(py)?);
        }

        Ok(manager
            .as_ref()
            .expect("manager initialized")
            .manager
            .clone_ref(py))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyDict;
    use std::mem::{size_of, MaybeUninit};

    macro_rules! scheduler_api_offset_of {
        ($field:ident) => {{
            let uninit = MaybeUninit::<SchedulerCapsuleApi>::uninit();
            let base = uninit.as_ptr();
            unsafe { std::ptr::addr_of!((*base).$field) as usize - base as usize }
        }};
    }

    #[test]
    fn reports_pyo3_smoke_status() {
        assert_eq!(bridge_status(), "pyo3_smoke");
        assert!(required_extension_module_names().contains(&"_scheduler"));
    }

    #[test]
    fn module_exports_initial_legacy_symbol_contract() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");

            for symbol in initial_public_symbols() {
                assert!(module.hasattr(*symbol).unwrap(), "missing symbol {symbol}");
            }

            let version: u32 = module
                .getattr("abi_version")
                .unwrap()
                .call0()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(version, carbon_scheduler_abi_version());
        });
    }

    #[test]
    fn callable_wrapper_public_type_matches_legacy_constructor_and_call_contract() {
        Python::with_gil(|py| {
            // Parity source:
            // carbonengine/scheduler/src/SchedulerModule.cpp adds
            // "callable_wrapper" during module init, and
            // carbonengine/scheduler/src/PyTasklet.cpp::CallableWrapperInit
            // accepts only callables while CallableWrapperCall delegates to the
            // wrapped callable.
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
assert scheduler.callable_wrapper.__name__ == "CallableWrapper"
assert scheduler.callable_wrapper.__module__ == "scheduler"
assert scheduler.callable_wrapper.__base__ is object
assert not hasattr(scheduler, "CallableWrapper")
wrapper = scheduler.callable_wrapper(lambda value, suffix="": f"{value}{suffix}")
assert wrapper("flight", suffix="-ready") == "flight-ready"
try:
    scheduler.callable_wrapper(object())
except TypeError as exc:
    assert str(exc) == "CallableWrapper only accepts a callable as an argument"
else:
    raise AssertionError("non-callable constructor argument should fail")
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("callable_wrapper should match legacy constructor/call surface");
        });
    }

    #[test]
    fn tasklet_exit_exception_module_matches_legacy_scheduler_module() {
        Python::with_gil(|py| {
            // Parity source:
            // carbonengine/scheduler/src/SchedulerModule.cpp creates
            // TaskletExit as "_scheduler.TaskletExit".
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
assert scheduler.TaskletExit.__module__ == "_scheduler"
assert issubclass(scheduler.TaskletExit, SystemExit)
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("TaskletExit should match legacy exception module/base");
        });
    }

    #[test]
    fn c_api_capsule_import_static_table_and_type_slots_match_legacy_scheduler_api() {
        Python::with_gil(|py| {
            // Parity sources:
            // - capiTest/InterpreterWithSchedulerModule.cpp::SetUp imports a
            //   build-flavor extension, aliases it into sys.modules["_scheduler"],
            //   imports scheduler, then calls SchedulerAPI().
            // - Scheduler.h::SchedulerAPI uses
            //   PyCapsule_Import("scheduler._C_API", 0) and caches the returned
            //   SchedulerCAPI pointer.
            // - SchedulerModule.cpp stores a static SchedulerCAPI table and
            //   points TaskletExit/type slots at the module objects.
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let capsule = scheduler.getattr("_C_API").unwrap();
            let direct = unsafe {
                ffi::PyCapsule_GetPointer(capsule.as_ptr(), scheduler_capsule_name_ptr())
                    .cast::<SchedulerCapsuleApi>()
            };
            if direct.is_null() {
                panic!(
                    "PyCapsule_GetPointer({SCHEDULER_CAPSULE_NAME}) failed: {:?}",
                    PyErr::fetch(py)
                );
            }

            let imported = unsafe {
                ffi::PyCapsule_Import(scheduler_capsule_name_ptr(), 0).cast::<SchedulerCapsuleApi>()
            };
            if imported.is_null() {
                panic!(
                    "PyCapsule_Import({SCHEDULER_CAPSULE_NAME}) failed: {:?}",
                    PyErr::fetch(py)
                );
            }
            assert_eq!(imported, direct);

            let api = unsafe { &*imported };
            assert_eq!(
                SCHEDULER_CAPSULE_NAME,
                carbon_scheduler_ffi::SCHEDULER_C_API_CAPSULE_NAME
            );
            assert_eq!(
                SCHEDULER_CAPSULE_C_NAME,
                carbon_scheduler_ffi::SCHEDULER_C_API_CAPSULE_NAME_CSTR
            );
            assert_eq!(
                size_of::<SchedulerCapsuleApi>(),
                size_of::<carbon_scheduler_ffi::SchedulerCapi>()
            );
            assert_eq!(
                size_of::<SchedulerCapsuleApi>(),
                carbon_scheduler_ffi::SCHEDULER_C_API_FIELD_COUNT * size_of::<*mut c_void>()
            );
            for (index, (name, offset)) in carbon_scheduler_ffi::SCHEDULER_C_API_FIELD_NAMES
                .iter()
                .zip(scheduler_c_api_offsets())
                .enumerate()
            {
                assert_eq!(
                    offset,
                    index * size_of::<*mut c_void>(),
                    "{name} offset should match Scheduler.h ABI order",
                );
            }
            for (name, pointer) in carbon_scheduler_ffi::SCHEDULER_C_API_FIELD_NAMES
                .iter()
                .zip(scheduler_c_api_entries(api))
            {
                assert!(!pointer.is_null(), "{name} entry should be populated");
            }

            assert!(!api.py_tasklet_type.is_null());
            assert!(!api.py_channel_type.is_null());
            assert!(!api.tasklet_exit.is_null());
            assert_eq!(
                api.py_tasklet_type.cast::<ffi::PyObject>(),
                scheduler.getattr("tasklet").unwrap().as_ptr()
            );
            assert_eq!(
                api.py_channel_type.cast::<ffi::PyObject>(),
                scheduler.getattr("channel").unwrap().as_ptr()
            );
            assert_eq!(
                unsafe { *api.tasklet_exit },
                scheduler.getattr("TaskletExit").unwrap().as_ptr()
            );

            let get_current: CApiPyObject =
                c_api_fn(api.py_scheduler_get_current, "PyScheduler_GetCurrent");
            let current = get_current();
            assert!(!current.is_null());
            unsafe {
                ffi::Py_DECREF(current);
            }

            let flavor_module = PyModule::new_bound(py, "_scheduler_debug").expect("create module");
            populate_scheduler_module(py, &flavor_module).expect("populate flavor module");
            let flavor_capsule = flavor_module.getattr("_C_API").unwrap();
            let flavor_direct = unsafe {
                ffi::PyCapsule_GetPointer(flavor_capsule.as_ptr(), scheduler_capsule_name_ptr())
                    .cast::<SchedulerCapsuleApi>()
            };
            if flavor_direct.is_null() {
                panic!(
                    "flavor PyCapsule_GetPointer({SCHEDULER_CAPSULE_NAME}) failed: {:?}",
                    PyErr::fetch(py)
                );
            }
            assert_eq!(
                flavor_direct, imported,
                "Scheduler.h consumers cache the SchedulerCAPI pointer, so module init must update a stable table in place",
            );
        });
    }

    #[test]
    fn callback_get_set_validation_matches_legacy_module_methods() {
        Python::with_gil(|py| {
            // Parity source:
            // SchedulerModule.cpp::{SetChannelCallback,SchedulerSetScheduleCallback}
            // require callable-or-None, return the previous callback, and the
            // unchanged legacy tests include
            // test_channel.TestChannels.test_set_channel_callback and
            // test_scheduler.TestSchedule.test_set_schedule_callback.
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
def callback_one(*args):
    return None

def callback_two(*args):
    return None

for prefix in ("channel", "schedule"):
    getter = getattr(scheduler, f"get_{prefix}_callback")
    setter = getattr(scheduler, f"set_{prefix}_callback")
    assert getter() is None
    assert setter(callback_one) is None
    assert getter() is callback_one
    assert setter(callback_two) is callback_one
    assert getter() is callback_two
    assert setter(None) is callback_two
    assert getter() is None
    try:
        setter(object())
    except TypeError as exc:
        assert str(exc) == "parameter must be callable or None."
    else:
        raise AssertionError(f"set_{prefix}_callback accepted a non-callable")
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("callback get/set should match legacy module contract");
        });
    }

    #[test]
    fn scheduler_misc_module_functions_match_legacy_return_contracts() {
        Python::with_gil(|py| {
            // Parity source:
            // SchedulerModule.cpp::EnableSoftSwitch only accepts None and
            // returns false, SchedulerSwitchTrap returns the previous level,
            // and SchedulerGetThreadInfo returns main/current/count.
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
assert scheduler.enable_softswitch(None) is False
try:
    scheduler.enable_softswitch(True)
except RuntimeError as exc:
    assert str(exc) == "enable_soft_switch is only implemented for legacy reasons, the value cannot be changed."
else:
    raise AssertionError("enable_softswitch accepted a changed value")

main = scheduler.getmain()
current = scheduler.getcurrent()
thread_info = scheduler.get_thread_info()
assert thread_info == (main, current, scheduler.getruncount() + 1)

try:
    assert scheduler.switch_trap(0) == 0
    assert scheduler.switch_trap(2) == 0
    assert scheduler.switch_trap(-1) == 2
    assert scheduler.switch_trap(-1) == 1
finally:
    while scheduler.switch_trap(0) != 0:
        scheduler.switch_trap(-1)
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("misc scheduler module functions should match legacy return contracts");
        });
    }

    #[test]
    fn channel_supports_subclass_constructor_shape() {
        Python::with_gil(|py| {
            // Parity source:
            // carbonengine/scheduler/src/PyChannel.cpp::ChannelNew and
            // ChannelInit construct exact base channels with default state.
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let channel_type = module.getattr("channel").unwrap();
            let channel = channel_type.call0().unwrap();

            assert_eq!(
                channel
                    .getattr("balance")
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                0
            );
            assert!(!channel
                .getattr("closing")
                .unwrap()
                .extract::<bool>()
                .unwrap());

            channel.setattr("preference", 1).unwrap();
            assert_eq!(
                channel
                    .getattr("preference")
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                1
            );
        });
    }

    #[test]
    fn py_channel_init_preference_and_weakref_match_legacy_wrapper() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
# Parity source: PyChannel.cpp::{ChannelNew,ChannelInit,
# PyChannelObjectIsValid,ChannelPreferenceSet} and PyChannel.h::m_weakrefList;
# legacy tests test_invalid_channel_when_skipping_init and
# test_invalid_channel_when_skipping_new.
import gc
import weakref
import weakref
import weakref

channel = scheduler.channel()
assert channel.preference == -1
channel.preference = 1
assert channel.preference == 1
channel.preference = 99
assert channel.preference == 1
channel.preference = -99
assert channel.preference == 1
try:
    channel.preference = object()
except TypeError as exc:
    assert str(exc) == "The first attribute value must be a number"
else:
    raise AssertionError("non-integer preference should fail")
try:
    del channel.preference
except TypeError as exc:
    assert str(exc) == "Cannot delete the first attribute"
else:
    raise AssertionError("preference deletion should fail")

ref = weakref.ref(channel)
assert ref() is channel
tmp = scheduler.channel()
tmp_ref = weakref.ref(tmp)
del tmp
for _ in range(3):
    gc.collect()
assert tmp_ref() is None

class SkipsInit(scheduler.channel):
    def __init__(self, *args, **kwargs):
        pass

invalid = SkipsInit()
try:
    invalid.send()
except RuntimeError as exc:
    assert str(exc) == "Channel object is not valid. Most likely cause being __init__ not called on base type."
else:
    raise AssertionError("subclass skipping base __init__ should be invalid")

class CallsInit(scheduler.channel):
    def __init__(self):
        super().__init__()

valid_subclass = CallsInit()
assert valid_subclass.balance == 0

class SkipsNew(scheduler.channel):
    def __new__(cls, *args, **kwargs):
        pass

assert SkipsNew() is None
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("channel init/preference/weakref behavior should match PyChannel.cpp");
        });
    }

    #[test]
    fn py_channel_close_open_and_iterator_errors_match_legacy_tests() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
# Parity source: PyChannel.cpp::{ChannelClose,ChannelOpen,ChannelNext}
# delegating to Channel.cpp::{Close,Open}; legacy tests
# test_send_on_closed, test_receive_on_closed, test_closing, test_open,
# and test_iterator_on_closed.
closed = scheduler.channel()
closed.close()
assert closed.closed is True
assert closed.closing is True
try:
    closed.send(None)
except ValueError as exc:
    assert str(exc) == "Send operation on a closed channel"
else:
    raise AssertionError("send on closed channel should fail")
try:
    closed.receive()
except ValueError as exc:
    assert str(exc) == "receive operation on a closed channel"
else:
    raise AssertionError("receive on closed channel should fail")
try:
    next(iter(closed))
except StopIteration as exc:
    assert exc.args == ("Channel is closed",)
else:
    raise AssertionError("closed channel iterator should stop")

closed.open()
assert closed.closed is False
assert closed.closing is False

drain = scheduler.channel()
def sender():
    drain.send(101)

sender_tasklet = scheduler.tasklet(sender)()
scheduler.run_n_tasklets(1)
assert drain.balance == 1
assert drain.queue is sender_tasklet
drain.close()
assert drain.closed is False
assert drain.closing is True
assert drain.receive() == 101
assert drain.closed is True
assert drain.closing is True
try:
    drain.receive()
except ValueError as exc:
    assert str(exc) == "receive operation on a closed channel"
else:
    raise AssertionError("drained closed channel should reject receive")
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("channel close/open and iterator errors should match legacy tests");
        });
    }

    #[test]
    fn py_channel_send_exception_and_send_throw_validation_match_legacy_wrapper() {
        Python::with_gil(|py| {
            let module = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
# Parity source: PyChannel.cpp::{ChannelSendException,ChannelSendThrow}
# and legacy tests test_send_exception, test_send_throw, and
# test_send_throw_prefence_send.
import gc
import sys
import traceback
while scheduler.switch_trap(0) > 0:
    scheduler.switch_trap(-1)
scheduler.set_channel_callback(None)
scheduler.set_schedule_callback(None)
for _ in range(2):
    try:
        scheduler.run()
    except Exception:
        pass
    scheduler.unblock_all_channels()
gc.collect()

channel = scheduler.channel()
try:
    channel.send_exception()
except RuntimeError as exc:
    assert str(exc) == "Exception type required", ("send_exception empty", str(exc))
else:
    raise AssertionError("send_exception without exc should fail")
try:
    channel.send_exception(object())
except RuntimeError as exc:
    assert str(exc) == "Exception type or instance required", ("send_exception object", str(exc))
else:
    raise AssertionError("send_exception should reject non-exceptions")
try:
    channel.send_throw(object())
except TypeError as exc:
    assert str(exc) == "Channel.send_throw() argument 'exc' (pos 1) must be an Exception type or instance", ("send_throw object", str(exc))
else:
    raise AssertionError("send_throw should reject non-exceptions")

exception_events = []
exception_channel = scheduler.channel()
def receive_exception():
    try:
        exception_channel.receive()
    except ValueError as exc:
        exc_type, exc_value, exc_tb = sys.exc_info()
        exception_events.append((
            exc_type is ValueError,
            exc_value is exc,
            exc.args,
            traceback.extract_tb(exc_tb)[-1][2],
        ))

receiver = scheduler.tasklet(receive_exception)()
scheduler.run_n_tasklets(1)
exception_channel.send_exception(ValueError, 1, 2, 3)
assert receiver.scheduled is False, ("receiver.scheduled", receiver.scheduled, receiver.alive, receiver.blocked, exception_channel.balance)
assert exception_events == [(True, True, (1, 2, 3), "receive_exception")], ("exception_events", exception_events)
assert receiver.alive is False, ("receiver.alive", receiver.alive)
assert exception_channel.balance == 0, ("exception_channel.balance", exception_channel.balance)

sender_throw_channel = scheduler.channel()
sender_throw_channel.preference = -1
def sender_bar():
    raise ValueError(1, 2, 3)

def sender_throw(test_channel):
    try:
        sender_bar()
    except Exception:
        test_channel.send_throw(*sys.exc_info())

sender_throw_tasklet = scheduler.tasklet(sender_throw)(sender_throw_channel)
assert scheduler.getruncount() == 2, ("sender runcount before", scheduler.getruncount())
sender_throw_tasklet.run()
assert scheduler.getruncount() == 1, ("sender runcount after run", scheduler.getruncount())
try:
    sender_throw_channel.receive()
except ValueError as exc:
    exc_type, exc_value, exc_tb = sys.exc_info()
    assert exc_type is ValueError, ("sender exc_type", exc_type)
    assert exc_value is exc, ("sender exc_value", exc_value, exc)
    assert exc.args == (1, 2, 3), ("sender args", exc.args)
    assert traceback.extract_tb(exc_tb)[-1][2] == "sender_bar", ("sender traceback", traceback.extract_tb(exc_tb))
else:
    raise AssertionError("send_throw from blocked sender did not restore ValueError")
assert scheduler.getruncount() == 2, ("sender runcount after receive", scheduler.getruncount())
scheduler.run()
assert sender_throw_tasklet.alive is False, ("sender alive", sender_throw_tasklet.alive)
assert sender_throw_channel.balance == 0, ("sender balance", sender_throw_channel.balance)

preference_events = []
preference_channel = scheduler.channel()
preference_channel.preference = 1
def preference_bar():
    raise ValueError(1, 2, 3)

def preference_receiver():
    try:
        preference_channel.receive()
    except ValueError as exc:
        exc_type, exc_value, exc_tb = sys.exc_info()
        preference_events.append((
            exc_type is ValueError,
            exc_value is exc,
            exc.args,
            traceback.extract_tb(exc_tb)[-1][2],
        ))

preference_receiver_tasklet = scheduler.tasklet(preference_receiver)()
scheduler.run_n_tasklets(1)
try:
    preference_bar()
except Exception:
    before_send_throw_info = sys.exc_info()
    preference_channel.send_throw(*before_send_throw_info)
    assert sys.exc_info()[1] is before_send_throw_info[1], ("preference exc_info", sys.exc_info(), before_send_throw_info)
assert preference_receiver_tasklet.scheduled is True, ("preference scheduled", preference_receiver_tasklet.scheduled)
scheduler.run()
assert preference_events == [(True, True, (1, 2, 3), "preference_bar")], ("preference_events", preference_events)
assert preference_receiver_tasklet.alive is False, ("preference alive", preference_receiver_tasklet.alive)
assert preference_channel.balance == 0, ("preference balance", preference_channel.balance)
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("send_exception/send_throw validation should match PyChannel.cpp");
        });
    }

    #[test]
    fn py_channel_queue_and_clear_visible_state_match_legacy_tests() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
# Parity source: PyChannel.cpp::{ChannelQueueGet,ChannelClearTasklets}
# delegating to Channel.cpp::{BlockedQueueFront,ClearBlocked}; legacy tests
# test_channel_send_queue_order, test_channel_receive_queue_order, and
# test_channel_test_clear_blocked.
send_channel = scheduler.channel()
def sender(value):
    send_channel.send(value)

send_one = scheduler.tasklet(sender)("one")
send_two = scheduler.tasklet(sender)("two")
scheduler.run()
assert send_channel.balance == 2
assert send_channel.queue is send_one
assert send_channel.receive() == "one"
assert send_channel.queue is send_two
send_channel.clear()
assert send_channel.balance == 0
assert send_channel.queue is None
assert send_two.blocked is False
assert send_two.alive is False

receive_channel = scheduler.channel()
def receiver():
    receive_channel.receive()

receive_one = scheduler.tasklet(receiver)()
receive_two = scheduler.tasklet(receiver)()
scheduler.run()
assert receive_channel.balance == -2
assert receive_channel.queue is receive_one
receive_channel.clear()
assert receive_channel.balance == 0
assert receive_channel.queue is None
assert receive_one.blocked is False
assert receive_one.alive is False
assert receive_two.blocked is False
assert receive_two.alive is False

exit_output = []
clear_channel = scheduler.channel()
def clear_sender(tasklet_id):
    try:
        clear_channel.send(tasklet_id)
    except scheduler.TaskletExit:
        exit_output.append(tasklet_id)

clear_one = scheduler.tasklet(clear_sender)("TASKLET1")
clear_two = scheduler.tasklet(clear_sender)("TASKLET2")
scheduler.run()
assert clear_channel.balance == 2
assert exit_output == []
clear_channel.clear()
assert clear_channel.balance == 0
assert clear_one.blocked is False
assert clear_one.alive is False
assert clear_two.blocked is False
assert clear_two.alive is False
assert exit_output == ["TASKLET1", "TASKLET2"], exit_output
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("channel queue and clear visible state should match legacy tests");
        });
    }

    #[test]
    fn py_channel_live_core_handles_mirror_blocked_sender_balance() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
channel = scheduler.channel()
def sender():
    channel.send("payload")

sender_tasklet = scheduler.tasklet(sender)()
sender_tasklet.run()
assert sender_tasklet.blocked is True
assert channel.balance == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("blocked sender should leave a mirrored core channel balance");

            let channel = locals.get_item("channel").unwrap().unwrap();
            let channel = channel.downcast::<Channel>().unwrap();
            {
                let channel = channel.try_borrow().unwrap();
                let snapshot = channel.core_snapshot().unwrap();
                assert!(channel.core_id.is_some());
                assert_eq!(channel.balance, 1);
                assert_eq!(snapshot.balance, 1);
                assert_eq!(snapshot.blocked_senders.len(), 1);
                assert!(snapshot.blocked_receivers.is_empty());
            }

            py.run_bound(
                r#"
assert channel.receive() == "payload"
assert channel.balance == 0
scheduler.run()
assert sender_tasklet.alive is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("receiving should drain the mirrored core channel balance");

            let channel = channel.try_borrow().unwrap();
            let snapshot = channel.core_snapshot().unwrap();
            assert_eq!(channel.balance, 0);
            assert_eq!(snapshot.balance, 0);
            assert!(snapshot.blocked_senders.is_empty());
            assert!(snapshot.blocked_receivers.is_empty());
        });
    }

    #[test]
    fn py_current_thread_run_queue_keeps_pyobjects_in_thread_local_registry() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
events = []
def tasklet_body():
    events.append("ran")

tasklet = scheduler.tasklet(tasklet_body)()
assert scheduler.getruncount() == 2
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("tasklet should schedule into the current thread queue");

            let thread_id = current_thread_id();
            let current_thread_queue_len = CURRENT_THREAD_RUN_QUEUE
                .with(|queue| queue.borrow().as_ref().map(|queue| queue.queue.len()));
            let global_queue_len = THREAD_RUN_QUEUES
                .lock()
                .expect("thread run queue lock poisoned")
                .iter()
                .find(|queue| queue.thread_id == thread_id)
                .map(|queue| queue.queue.len());

            assert_eq!(current_thread_queue_len, Some(1));
            assert_eq!(global_queue_len, Some(0));

            py.run_bound(
                r#"
scheduler.run()
assert events == ["ran"], events
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("thread-local queue should still drain through CoreScheduler order");
        });
    }

    #[test]
    fn py_channel_public_state_getters_read_core_snapshots() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
channel = scheduler.channel()
channel.preference = 1
channel.close()
assert channel.preference == 1
assert channel.closing is True
assert channel.closed is True
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("channel core state should be visible before bridge-local drift");

            let channel = locals.get_item("channel").unwrap().unwrap();
            let channel = channel.downcast::<Channel>().unwrap();
            {
                let mut channel = channel.try_borrow_mut().unwrap();
                channel.preference = -1;
                channel.closing = false;
                channel.closed = false;
            }

            py.run_bound(
                r#"
assert channel.preference == 1
assert channel.closing is True
assert channel.closed is True
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("public channel getters should read core snapshot, not drifted bridge fields");

            {
                let channel = channel.try_borrow().unwrap();
                bridge_core_open_channel(channel.core_channel_id().unwrap()).unwrap();
            }
            {
                let mut channel = channel.try_borrow_mut().unwrap();
                channel.preference = -1;
                channel.closing = true;
                channel.closed = true;
            }

            py.run_bound(
                r#"
assert channel.preference == 1
assert channel.closing is False
assert channel.closed is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("opened channel getters should remain core-authoritative");
        });
    }

    #[test]
    fn py_channel_transfer_selection_uses_core_ids_when_python_queue_order_drifts() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
receive_events = []
receive_channel = scheduler.channel()
def receiver(name):
    value = receive_channel.receive()
    receive_events.append((name, value))

receiver_one = scheduler.tasklet(receiver)("one")
receiver_two = scheduler.tasklet(receiver)("two")
scheduler.run()
assert receive_channel.balance == -2
assert receive_channel.queue is receiver_one
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("two receivers should block in core order");

            let receive_channel_object = locals
                .get_item("receive_channel")
                .unwrap()
                .unwrap()
                .to_object(py);
            let receiver_one = locals
                .get_item("receiver_one")
                .unwrap()
                .unwrap()
                .to_object(py);
            let receiver_two = locals
                .get_item("receiver_two")
                .unwrap()
                .unwrap()
                .to_object(py);
            let receiver_one_core = tasklet_core_id(py, &receiver_one).unwrap();
            let receiver_two_core = tasklet_core_id(py, &receiver_two).unwrap();
            {
                let receive_channel = receive_channel_object
                    .bind(py)
                    .downcast::<Channel>()
                    .unwrap();
                let mut receive_channel = receive_channel.try_borrow_mut().unwrap();
                let snapshot = receive_channel.core_snapshot().unwrap();
                assert_eq!(
                    snapshot.blocked_receivers,
                    vec![receiver_one_core, receiver_two_core]
                );
                receive_channel.blocked_receivers.swap(0, 1);
                let python_front = receive_channel.blocked_receivers.front().unwrap();
                assert_eq!(
                    tasklet_core_id(py, python_front).unwrap(),
                    receiver_two_core
                );
            }

            py.run_bound(
                r#"
assert receive_channel.queue is receiver_one
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("queue getter should use CoreScheduler receiver front");

            py.run_bound(
                r#"
receive_channel.send("payload")
scheduler.run()
assert receive_events == [("one", "payload")], receive_events
assert receiver_one.blocked is False
assert receiver_two.blocked is True
assert receive_channel.balance == -1
receive_channel.clear()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("send should continue the receiver selected by CoreScheduler");

            py.run_bound(
                r#"
send_events = []
send_channel = scheduler.channel()
def sender(name):
    send_channel.send(name)
    send_events.append(name)

sender_one = scheduler.tasklet(sender)("one")
sender_two = scheduler.tasklet(sender)("two")
scheduler.run()
assert send_channel.balance == 2
assert send_channel.queue is sender_one
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("two senders should block in core order");

            let send_channel_object = locals
                .get_item("send_channel")
                .unwrap()
                .unwrap()
                .to_object(py);
            let sender_one = locals
                .get_item("sender_one")
                .unwrap()
                .unwrap()
                .to_object(py);
            let sender_two = locals
                .get_item("sender_two")
                .unwrap()
                .unwrap()
                .to_object(py);
            let sender_one_core = tasklet_core_id(py, &sender_one).unwrap();
            let sender_two_core = tasklet_core_id(py, &sender_two).unwrap();
            {
                let send_channel = send_channel_object.bind(py).downcast::<Channel>().unwrap();
                let mut send_channel = send_channel.try_borrow_mut().unwrap();
                let snapshot = send_channel.core_snapshot().unwrap();
                assert_eq!(
                    snapshot.blocked_senders,
                    vec![sender_one_core, sender_two_core]
                );
                send_channel.blocked_senders.swap(0, 1);
                let python_front = send_channel.blocked_senders.front().unwrap();
                assert_eq!(tasklet_core_id(py, python_front).unwrap(), sender_two_core);
            }

            py.run_bound(
                r#"
assert send_channel.queue is sender_one
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("queue getter should use CoreScheduler sender front");

            py.run_bound(
                r#"
assert send_channel.receive() == "one"
scheduler.run()
assert send_events == ["one"], send_events
assert sender_one.blocked is False
assert sender_two.blocked is True
assert send_channel.balance == 1
send_channel.clear()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("receive should continue the sender selected by CoreScheduler");
        });
    }

    #[test]
    fn py_tasklet_live_core_snapshot_mirrors_setup_run_completion() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
events = []
def record():
    events.append("ran")

tasklet = scheduler.tasklet(record)
tasklet()
assert tasklet.alive is True
assert tasklet.scheduled is True
assert tasklet.paused is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("scheduled tasklet should be visible before execution");

            let tasklet = locals.get_item("tasklet").unwrap().unwrap();
            let tasklet = tasklet.downcast::<Tasklet>().unwrap();
            {
                let tasklet = tasklet.try_borrow().unwrap();
                let snapshot = tasklet.core_snapshot().unwrap();
                assert_eq!(snapshot.lifecycle, CoreTaskletLifecycle::Runnable);
                assert!(snapshot.alive);
                assert!(snapshot.scheduled);
                assert!(!snapshot.paused);
                assert_eq!(snapshot.times_switched_to, 0);
            }

            py.run_bound(
                r#"
scheduler.run_n_tasklets(1)
assert events == ["ran"]
assert tasklet.alive is False
assert tasklet.scheduled is False
assert tasklet.paused is False
assert tasklet.times_switched_to == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("completed tasklet should update Python-visible lifecycle");

            let tasklet = tasklet.try_borrow().unwrap();
            let snapshot = tasklet.core_snapshot().unwrap();
            assert_eq!(snapshot.lifecycle, CoreTaskletLifecycle::Complete);
            assert!(!snapshot.alive);
            assert!(!snapshot.scheduled);
            assert!(!snapshot.paused);
            assert_eq!(snapshot.times_switched_to, 1);
        });
    }

    #[test]
    fn py_tasklet_public_state_getters_read_core_snapshots() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
events = []
def record():
    events.append("ran")

tasklet = scheduler.tasklet(record)
tasklet()
assert tasklet.alive is True
assert tasklet.scheduled is True
assert tasklet.paused is False
assert tasklet.block_trap is False
assert tasklet.times_switched_to == 0
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("tasklet should be scheduled before bridge-local drift");

            let tasklet = locals.get_item("tasklet").unwrap().unwrap();
            let tasklet = tasklet.downcast::<Tasklet>().unwrap();
            {
                let mut tasklet = tasklet.try_borrow_mut().unwrap();
                tasklet.alive = false;
                tasklet.scheduled = false;
                tasklet.paused = true;
                tasklet.block_trap = true;
                tasklet.times_switched_to = 99;
            }

            py.run_bound(
                r#"
assert tasklet.alive is True
assert tasklet.scheduled is True
assert tasklet.paused is False
assert tasklet.block_trap is False
assert tasklet.times_switched_to == 0
tasklet.block_trap = True
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("public getters should read core snapshot, not drifted bridge fields");

            {
                let mut tasklet = tasklet.try_borrow_mut().unwrap();
                tasklet.block_trap = false;
            }
            py.run_bound(
                r#"
assert tasklet.block_trap is True
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("block_trap getter should remain core-authoritative after setter sync");

            py.run_bound(
                r#"
channel = scheduler.channel()
def receive():
    channel.receive()

receiver = scheduler.tasklet(receive)
receiver()
receiver.run()
assert receiver.blocked is True
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("receiver should block through core channel state");

            let receiver = locals.get_item("receiver").unwrap().unwrap();
            let receiver = receiver.downcast::<Tasklet>().unwrap();
            {
                let mut receiver = receiver.try_borrow_mut().unwrap();
                receiver.blocked = false;
            }
            py.run_bound(
                r#"
assert receiver.blocked is True
channel.clear()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("blocked getter should read core blocked_on state");
        });
    }

    #[test]
    fn py_tasklet_paused_core_snapshot_drives_bind_remove_insert_and_direct_run() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
events = []
paused = scheduler.tasklet(lambda value: events.append(value))
paused.bind(args=(7,))
assert paused.alive is True
assert paused.scheduled is False
assert paused.paused is True
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("bound args should create a paused tasklet");

            let paused = locals.get_item("paused").unwrap().unwrap();
            let paused = paused.downcast::<Tasklet>().unwrap();
            {
                let tasklet = paused.try_borrow().unwrap();
                let snapshot = tasklet.core_snapshot().unwrap();
                assert_eq!(snapshot.lifecycle, CoreTaskletLifecycle::Runnable);
                assert!(snapshot.alive);
                assert!(!snapshot.scheduled);
                assert!(snapshot.paused);
            }

            py.run_bound(
                r#"
paused.run()
assert events == [7]
assert paused.alive is False
assert paused.scheduled is False
assert paused.paused is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("direct run should accept a core-paused tasklet");
            {
                let tasklet = paused.try_borrow().unwrap();
                let snapshot = tasklet.core_snapshot().unwrap();
                assert_eq!(snapshot.lifecycle, CoreTaskletLifecycle::Complete);
                assert!(!snapshot.alive);
                assert!(!snapshot.scheduled);
                assert!(!snapshot.paused);
            }

            py.run_bound(
                r#"
removed = scheduler.tasklet(lambda: events.append("removed"))()
assert removed.remove() is removed
assert removed.alive is True
assert removed.scheduled is False
assert removed.paused is True
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("remove should pause through CoreScheduler");

            let removed = locals.get_item("removed").unwrap().unwrap();
            let removed = removed.downcast::<Tasklet>().unwrap();
            {
                let tasklet = removed.try_borrow().unwrap();
                let snapshot = tasklet.core_snapshot().unwrap();
                assert_eq!(snapshot.lifecycle, CoreTaskletLifecycle::Runnable);
                assert!(snapshot.alive);
                assert!(!snapshot.scheduled);
                assert!(snapshot.paused);
            }

            py.run_bound(
                r#"
removed.insert()
assert removed.alive is True
assert removed.scheduled is True
assert removed.paused is False
assert scheduler.getruncount() == 2
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("insert should resume and schedule through CoreScheduler");
            {
                let tasklet = removed.try_borrow().unwrap();
                let snapshot = tasklet.core_snapshot().unwrap();
                assert_eq!(snapshot.lifecycle, CoreTaskletLifecycle::Runnable);
                assert!(snapshot.alive);
                assert!(snapshot.scheduled);
                assert!(!snapshot.paused);
            }

            py.run_bound(
                r#"
scheduler.run()
assert events == [7, "removed"]
assert removed.alive is False
assert removed.scheduled is False
assert removed.paused is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("rescheduled removed tasklet should run unchanged");
        });
    }

    #[test]
    fn py_channel_remaining_blocking_deadlock_and_block_trap_tests_match_legacy() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_channel.py::TestChannels::
# test_main_tasklet_blocking_without_a_sender,
# test_main_tasklet_blocking_without_receiver,
# test_main_tasklet_receive_deadlock_after_running_child_tasklets,
# test_main_tasklet_send_deadlock_after_running_child_tasklets,
# test_blocking_receive_on_main_tasklet,
# test_blocking_send_on_main_tasklet,
# test_blocked_tasklets_greenlet_is_not_parent, and the two
# block_trap balance no-mutation tests.
try:
    scheduler.run()
except Exception:
    pass
scheduler.unblock_all_channels()

deadlock_channel = scheduler.channel()
try:
    deadlock_channel.receive()
except RuntimeError as exc:
    assert "Deadlock" in str(exc), str(exc)
else:
    raise AssertionError("main receive without sender should deadlock")

deadlock_channel = scheduler.channel()
try:
    deadlock_channel.send(1)
except RuntimeError as exc:
    assert "Deadlock" in str(exc), str(exc)
else:
    raise AssertionError("main send without receiver should deadlock")

run_order = []
def noop(i):
    run_order.append(i)
for i in range(10):
    scheduler.tasklet(noop)(i)
chan = scheduler.channel()
try:
    chan.receive()
except RuntimeError as exc:
    assert "Deadlock" in str(exc), str(exc)
else:
    raise AssertionError("main receive should deadlock after draining children")
assert run_order == list(range(10)), run_order

run_order = []
for i in range(10):
    scheduler.tasklet(noop)(i)
chan = scheduler.channel()
try:
    chan.send(1)
except RuntimeError as exc:
    assert "Deadlock" in str(exc), str(exc)
else:
    raise AssertionError("main send should deadlock after draining children")
assert run_order == list(range(10)), run_order

sent_values = []
def sender(chan):
    for i in range(10):
        chan.send(i)
        sent_values.append(i)

channel = scheduler.channel()
sending_tasklet = scheduler.tasklet(sender)(channel)
sending_tasklet.run()
assert sent_values == []
assert channel.balance == 1
received_values = []
for i in range(10):
    received_values.append(channel.receive())
assert received_values == list(range(10)), received_values
assert channel.balance == 0
scheduler.run()
assert sent_values == list(range(10)), sent_values

received_values = []
def receiver(chan):
    for i in range(10):
        received_values.append(chan.receive())

channel = scheduler.channel()
receiving_tasklet = scheduler.tasklet(receiver)(channel)
receiving_tasklet.run()
assert received_values == []
assert channel.balance == -1
for i in range(10):
    channel.send(i)
assert channel.balance == 0
assert received_values == list(range(10)), received_values

tasklet_order = []
def foo(x):
    tasklet_order.append(x)

channel = scheduler.channel()
def ordered_sender(chan):
    scheduler.tasklet(foo)("a")
    chan.send(1)
    scheduler.tasklet(foo)("b")
    chan.send(2)
    scheduler.tasklet(foo)("c")
    chan.send(3)
    scheduler.tasklet(foo)("d")

sender_tasklet = scheduler.tasklet(ordered_sender)(channel)
sender_tasklet.run()
assert scheduler.getruncount() == 2, scheduler.getruncount()
tasklet_order.append(channel.receive())
tasklet_order.append(channel.receive())
tasklet_order.append(channel.receive())
assert tasklet_order == [1, "a", 2, "b", 3], tasklet_order
scheduler.run()

channel = scheduler.channel()
old_block_trap = scheduler.getcurrent().block_trap
scheduler.getcurrent().block_trap = True
try:
    try:
        channel.send(1)
    except RuntimeError:
        pass
    else:
        raise AssertionError("block-trapped send should fail")
finally:
    scheduler.getcurrent().block_trap = old_block_trap
assert channel.balance == 0

channel = scheduler.channel()
old_block_trap = scheduler.getcurrent().block_trap
scheduler.getcurrent().block_trap = True
try:
    try:
        channel.receive()
    except RuntimeError:
        pass
    else:
        raise AssertionError("block-trapped receive should fail")
finally:
    scheduler.getcurrent().block_trap = old_block_trap
assert channel.balance == 0

scheduler.run()
scheduler.unblock_all_channels()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("remaining blocking/deadlock/block_trap channel tests should match legacy");
        });
    }

    #[test]
    fn py_channel_preference_handoff_order_matches_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_channel.py::TestChannels::
# test_sending_tasklets_rescheduled_by_channel_are_run,
# test_receiving_tasklets_rescheduled_by_channel_are_run,
# test_preference_receiver, test_preference_sender, and
# test_preference_neither_simple.
try:
    scheduler.run()
except Exception:
    pass
scheduler.unblock_all_channels()

run_order = []
def sender(chan, x):
    chan.send(x)
    run_order.append(2)
def receiver(chan):
    chan.receive()
    run_order.append(1)
channel = scheduler.channel()
scheduler.tasklet(receiver)(channel)
scheduler.tasklet(sender)(channel, "Joe")
scheduler.run()
assert run_order == [1, 2], run_order

run_order = []
channel = scheduler.channel()
scheduler.tasklet(sender)(channel, "Joe")
scheduler.tasklet(receiver)(channel)
scheduler.run()
assert run_order == [1, 2], run_order

run_order = []
def sender_preferred(chan, x):
    chan.send(x)
    run_order.append(1)
def receiver_preferred(chan):
    chan.receive()
    run_order.append(2)
channel = scheduler.channel()
channel.preference = 1
scheduler.tasklet(receiver_preferred)(channel)
scheduler.tasklet(sender_preferred)(channel, "Joe")
scheduler.run()
assert run_order == [1, 2], run_order

run_order = []
channel = scheduler.channel()
channel.preference = 1
scheduler.tasklet(sender_preferred)(channel, "Joe")
scheduler.tasklet(receiver_preferred)(channel)
scheduler.run()
assert run_order == [1, 2], run_order

completed_send_tasklets = []
channel = scheduler.channel()
channel.preference = -1
def preference_receiver_sender(chan, x):
    chan.send(x)
    completed_send_tasklets.append(x)
for i in range(10):
    scheduler.tasklet(preference_receiver_sender)(channel, i).run()
for i in range(10):
    assert channel.receive() == i
    assert completed_send_tasklets == [], completed_send_tasklets
scheduler.run()
assert completed_send_tasklets == list(range(10)), completed_send_tasklets

completed_tasklets = []
channel = scheduler.channel()
channel.preference = 1
def preference_sender_sender(chan, x):
    chan.send("test")
    completed_tasklets.append(("sender", x))
def preference_sender_receiver(chan, x):
    chan.receive()
    completed_tasklets.append(("receiver", x))
for i in range(3):
    scheduler.tasklet(preference_sender_sender)(channel, i)
for i in range(3):
    scheduler.tasklet(preference_sender_receiver)(channel, i)
scheduler.run()
assert completed_tasklets == [
    ("sender", 0), ("sender", 1), ("sender", 2),
    ("receiver", 0), ("receiver", 1), ("receiver", 2),
], completed_tasklets

completed_tasklets = []
channel = scheduler.channel()
channel.preference = 0
def neither_receiver():
    channel.receive()
    completed_tasklets.append("receiver_after_receive")
scheduler.tasklet(neither_receiver)()
scheduler.run()
assert completed_tasklets == []
channel.send(None)
assert completed_tasklets == []
scheduler.run()
assert completed_tasklets == ["receiver_after_receive"], completed_tasklets
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("channel preference handoff order should match legacy tests");
        });
    }

    #[test]
    fn py_channel_kill_raise_and_pending_cleanup_match_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_channel.py::TestChannels::
# test_kill_tasklet_blocked_on_channel_receive,
# test_kill_tasklet_blocked_on_channel_send,
# test_pending_kill_blocked_receive_tasklet,
# test_pending_kill_blocked_send_tasklet,
# test_kill_blocked_on_send_on_closed,
# test_kill_blocked_on_receive_on_closed,
# test_raise_exception_blocked_on_send_on_closed, and
# test_raise_exception_blocked_on_receive_on_closed.
try:
    scheduler.run()
except Exception:
    pass
scheduler.unblock_all_channels()

def receive(c):
    c.receive()
def send(c):
    c.send(1)

channel = scheduler.channel()
t = scheduler.tasklet(receive)(channel)
scheduler.run()
assert channel.balance == -1
t.kill()
assert channel.balance == 0
assert t.alive is False

channel = scheduler.channel()
t = scheduler.tasklet(send)(channel)
scheduler.run()
assert channel.balance == 1
t.kill()
assert channel.balance == 0
assert t.alive is False

channel = scheduler.channel()
t = scheduler.tasklet(receive)(channel)
t.run()
assert channel.balance == -1
t.kill(pending=True)
assert channel.balance == 0
assert t.alive is True
scheduler.run()
assert t.alive is False

channel = scheduler.channel()
t = scheduler.tasklet(send)(channel)
t.run()
assert channel.balance == 1
t.kill(pending=True)
assert channel.balance == 0
assert t.alive is True
scheduler.run()
assert t.alive is False

channel = scheduler.channel()
def send_none():
    channel.send(None)
t1 = scheduler.tasklet(send_none)()
t2 = scheduler.tasklet(send_none)()
scheduler.run()
channel.close()
assert channel.closed is False
t1.kill()
assert channel.closed is False
t2.kill()
assert channel.closed is True
assert channel.balance == 0

channel = scheduler.channel()
def receive_none():
    channel.receive()
t1 = scheduler.tasklet(receive_none)()
t2 = scheduler.tasklet(receive_none)()
scheduler.run()
channel.close()
assert channel.closed is False
t1.kill()
assert channel.closed is False
t2.kill()
assert channel.closed is True
assert channel.balance == 0

channel = scheduler.channel()
t1 = scheduler.tasklet(send_none)()
t2 = scheduler.tasklet(send_none)()
scheduler.run()
channel.close()
assert channel.closed is False
t1.raise_exception(scheduler.TaskletExit)
assert channel.closed is False
t2.raise_exception(scheduler.TaskletExit)
assert channel.closed is True
assert channel.balance == 0

channel = scheduler.channel()
t1 = scheduler.tasklet(receive_none)()
t2 = scheduler.tasklet(receive_none)()
scheduler.run()
channel.close()
assert channel.closed is False
t1.raise_exception(scheduler.TaskletExit)
assert channel.closed is False
t2.raise_exception(scheduler.TaskletExit)
assert channel.closed is True
assert channel.balance == 0

scheduler.run()
scheduler.unblock_all_channels()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("channel kill/raise cleanup tests should match legacy");
        });
    }

    #[test]
    fn nested_tasklet_flag_round_trips() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");

            module
                .getattr("set_use_nested_tasklets")
                .unwrap()
                .call1((false,))
                .unwrap();
            assert!(!module
                .getattr("get_use_nested_tasklets")
                .unwrap()
                .call0()
                .unwrap()
                .extract::<bool>()
                .unwrap());

            module
                .getattr("set_use_nested_tasklets")
                .unwrap()
                .call1((true,))
                .unwrap();
        });
    }

    #[test]
    fn python_tasklet_call_and_run_n_execute_simple_callable() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();

            py.run_bound(
                r#"
values = []
tasklet = scheduler.tasklet(lambda value: values.append(value))(7)
assert scheduler.getruncount() == 2
assert tasklet.alive is True
assert tasklet.scheduled is True
scheduler.run_n_tasklets(1)
assert values == [7]
assert scheduler.getruncount() == 1
assert tasklet.alive is False
assert tasklet.scheduled is False
assert tasklet.times_switched_to == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("simple tasklet should run through run_n_tasklets");
        });
    }

    #[test]
    fn legacy_scheduler_package_reexports_rust_surface_and_queue_channel_wrapper() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");

            assert!(scheduler.hasattr("QueueChannel").unwrap());
            assert!(scheduler.hasattr("_C_API").unwrap());
            assert!(scheduler.hasattr("channel").unwrap());

            let queue = scheduler
                .getattr("QueueChannel")
                .unwrap()
                .call0()
                .expect("construct QueueChannel");
            let channel_type = scheduler.getattr("channel").unwrap();
            assert!(queue.is_instance(&channel_type).unwrap());
            assert_eq!(
                queue
                    .getattr("preference")
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                1
            );

            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();
            locals.set_item("queue", &queue).unwrap();

            py.run_bound(
                r#"
import gc
while scheduler.switch_trap(0) > 0:
    scheduler.switch_trap(-1)
scheduler.set_channel_callback(None)
scheduler.set_schedule_callback(None)
for _ in range(2):
    try:
        scheduler.run()
    except Exception:
        pass
    scheduler.unblock_all_channels()
gc.collect()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("scheduler state should be clean before QueueChannel parity checks");

            assert_eq!(py_len(py, &locals, "queue"), 0);
            queue.call_method1("send", ("alpha",)).unwrap();
            assert_eq!(
                queue.getattr("balance").unwrap().extract::<i32>().unwrap(),
                1
            );
            assert_eq!(py_len(py, &locals, "queue"), 1);
            py.run_bound(
                r#"
# Parity source: scheduler/__init__.py::QueueChannel.data_queue
# and test_queuechannel.py::TestQueueChannels.test_queue_data.
assert list(queue.data_queue) == [(True, "alpha")]
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("QueueChannel should store buffered values in data_queue");
            assert_eq!(
                queue
                    .call_method0("receive")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "alpha"
            );
            assert_eq!(py_len(py, &locals, "queue"), 0);

            py.run_bound(
                r#"
# Parity source: scheduler/__init__.py::QueueChannel.{send_sequence,
# __next__,__len__,send_exception,send_throw,receive}; legacy tests
# test_queuechannel.py::{test_queue_data,test_send_exception,test_send_throw}.
import sys
import traceback
queue.send_sequence(range(3))
assert len(queue) == 3
assert queue.balance == 3
assert next(queue) == 0
assert queue.receive() == 1
assert next(iter(queue)) == 2
assert len(queue) == 0

queue_exception_events = []
queue.send_exception(ValueError, 1, 2, 3)
try:
    queue.receive()
except ValueError as exc:
    exc_type, exc_value, exc_tb = sys.exc_info()
    queue_exception_events.append((
        exc_type is ValueError,
        exc_value is exc,
        exc.args,
        traceback.extract_tb(exc_tb)[-1][2],
    ))
else:
    raise AssertionError("QueueChannel did not raise queued send_exception payload")
assert queue_exception_events == [(True, True, ((1, 2, 3),), "receive")], queue_exception_events

def queue_bar():
    raise ValueError(1, 2, 3)

try:
    queue_bar()
except Exception:
    queued_throw_info = sys.exc_info()
    queue.send_throw(*sys.exc_info())
    assert sys.exc_info()[1] is queued_throw_info[1]
try:
    queue.receive()
except ValueError as exc:
    exc_type, exc_value, exc_tb = sys.exc_info()
    assert exc_type is ValueError
    assert exc_value is exc
    assert exc.args == (queued_throw_info[1],), exc.args
    assert traceback.extract_tb(exc_tb)[-1][2] == "queue_bar"
else:
    raise AssertionError("QueueChannel did not raise queued send_throw payload")

# Parity source: test_queuechannel.py::TestQueueChannels.test_blocking_receive
# and scheduler/__init__.py::QueueChannel.send direct handoff when a receiver
# is already blocked on the base channel.
handoff = scheduler.QueueChannel()
def receive(test_channel):
    return test_channel.receive()
def send(test_channel):
    test_channel.send((1, 2, 3))

receiving_tasklet = scheduler.tasklet(receive)(handoff)
assert scheduler.getruncount() == 2
receiving_tasklet.run()
assert scheduler.getruncount() == 1
assert receiving_tasklet.blocked is True
assert handoff.balance == -1
assert len(handoff) == 0

sending_tasklet = scheduler.tasklet(send)(handoff)
assert scheduler.getruncount() == 2
sending_tasklet.run()
assert scheduler.getruncount() == 2
assert receiving_tasklet.blocked is False
assert receiving_tasklet.scheduled is True
assert receiving_tasklet.alive is True
assert sending_tasklet.alive is False
assert handoff.balance == 0
assert len(handoff) == 0
try:
    receiving_tasklet.kill()
except scheduler.TaskletExit:
    pass
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("QueueChannel wrapper behavior should match legacy queuechannel tests");

            py.run_bound(
                r#"
import gc
while scheduler.switch_trap(0) > 0:
    scheduler.switch_trap(-1)
scheduler.set_channel_callback(None)
scheduler.set_schedule_callback(None)
for _ in range(2):
    try:
        scheduler.run()
    except Exception:
        pass
    scheduler.unblock_all_channels()
gc.collect()

assert scheduler.getcurrent().block_trap is False
with scheduler.block_trap():
    assert scheduler.getcurrent().block_trap is True
assert scheduler.getcurrent().block_trap is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("block_trap context manager should round trip current tasklet flag");

            CURRENT_TASKLET.with(|cell| {
                cell.borrow_mut().take();
            });
        });
    }

    #[test]
    fn tasklet_gc_traverses_callable_cycles_like_legacy_wrapper() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: PyTasklet.cpp TaskletTraverse/TaskletClear and
# test_tasklet.py::TestTasklets.test_cyclical_callable_reference_cleans_up.
import gc
import weakref

class DummyWrapper:
    def __init__(self):
        def inner():
            return self
        self.tasklet = scheduler.tasklet(inner)

wrapper = DummyWrapper()
wrapper_ref = weakref.ref(wrapper)
tasklet_ref = weakref.ref(wrapper.tasklet)
wrapper = None
for _ in range(3):
    gc.collect()
assert wrapper_ref() is None
assert tasklet_ref() is None
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("tasklet callable cycles should be visible to Python GC");
        });
    }

    #[test]
    fn tasklet_getset_properties_match_legacy_py_tasklet_surface() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();
            locals
                .set_item("__name__", "tasklet_property_parity")
                .unwrap();

            py.run_bound(
                r#"
# Parity source: PyTasklet.cpp Tasklet_getsetters,
# Utils.cpp::StdStringFromPyObject, Tasklet.cpp::SetCallsiteData,
# and test_tasklet.py::TestTaskletMetricsCollection.
def sample_callable():
    return None

t = scheduler.tasklet(sample_callable)
assert t.method_name == "sample_callable"
assert t.module_name == "tasklet_property_parity"
assert t.file_name == sample_callable.__code__.co_filename
assert t.line_number == sample_callable.__code__.co_firstlineno

t.context = "context-value"
assert t.context == "context-value"
t.parent_callsite = "parent-site"
assert t.parent_callsite == "parent-site"
for attr in ("context", "parent_callsite"):
    try:
        setattr(t, attr, object())
    except TypeError as exc:
        assert str(exc) == "value must be a string", (attr, exc)
    else:
        raise AssertionError(f"{attr} accepted a non-string value")

assert t.highlighted is False
t.highlighted = True
assert t.highlighted is True
try:
    t.highlighted = 1
except TypeError as exc:
    assert "highlighted" in str(exc)
else:
    raise AssertionError("highlighted accepted a non-bool value")

t.runTime = 12.5
assert t.runTime == 12.5

try:
    t.frame
except RuntimeError as exc:
    assert "frame Not implemented" in str(exc)
else:
    raise AssertionError("frame getter did not raise RuntimeError")

try:
    scheduler.tasklet(object())
except TypeError as exc:
    assert "callable" in str(exc)
else:
    raise AssertionError("tasklet constructor accepted a non-callable")
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("tasklet get/set properties should match the legacy wrapper");
        });
    }

    #[test]
    fn tasklet_dont_raise_context_manager_and_handler_match_legacy_wrapper() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: PyTasklet.cpp CallableWrapperCall and
# test_tasklet.py::TestTaskletDontRaise.{test_tasklet_dont_raise,
# test_raising_tasklet_with_tracer,test_exception_handler}.
events = []
handler_types = []

class CtxMgr:
    def __init__(self, tasklet):
        self.tasklet = tasklet

    def __enter__(self):
        events.append(("enter", self.tasklet.context))

    def __exit__(self, exctype, excinst, exctb):
        events.append(("exit", exctype, excinst, exctb))

def exception_handler(_info):
    import sys
    handler_types.append(sys.exc_info()[0])

def raises_type_error():
    events.append(("call", None))
    raise TypeError("boom")

t = scheduler.tasklet()
t.context = "handler-context"
t.dont_raise = True
t.bind(raises_type_error)
t.context_manager_getter = CtxMgr
t.exception_handler = exception_handler
t.setup()
scheduler.run()

assert events == [
    ("enter", "handler-context"),
    ("call", None),
    ("exit", None, None, None),
]
assert handler_types == [TypeError]
assert t.alive is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("dont_raise wrapper behavior should match legacy CallableWrapper");
        });
    }

    #[test]
    fn tasklet_dont_raise_context_manager_error_edges_match_legacy_wrapper() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: PyTasklet.cpp CallableWrapperCall and
# test_tasklet.py::TestTaskletDontRaise.{test_tasklet_with_raising_tracer_enter,
# test_tasklet_with_raising_tracer_exit,test_exception_handler_raises}.
events = []

def body():
    events.append("body")

class RaisingEnter:
    def __init__(self, tasklet):
        self.tasklet = tasklet

    def __enter__(self):
        events.append("enter")
        raise TypeError("enter failed")

    def __exit__(self, exctype, excinst, exctb):
        events.append("exit")

t = scheduler.tasklet()
t.dont_raise = True
t.context_manager_getter = RaisingEnter
t.bind(body)
t.setup()
try:
    scheduler.run()
except TypeError as exc:
    assert str(exc) == "enter failed"
else:
    raise AssertionError("__enter__ failure should propagate")
assert events == ["enter"]
assert t.alive is False

events.clear()

class RaisingExit:
    def __init__(self, tasklet):
        self.tasklet = tasklet

    def __enter__(self):
        events.append("enter")

    def __exit__(self, exctype, excinst, exctb):
        events.append(("exit", exctype, excinst, exctb))
        raise TypeError("exit failed")

t = scheduler.tasklet()
t.dont_raise = True
t.context_manager_getter = RaisingExit
t.bind(body)
t.setup()
try:
    scheduler.run()
except TypeError as exc:
    assert str(exc) == "exit failed"
else:
    raise AssertionError("__exit__ failure should propagate")
assert events == ["enter", "body", ("exit", None, None, None)]
assert t.alive is False

events.clear()
handler_types = []

def raises_value_error():
    events.append("body_error")
    raise ValueError("body failed")

def exception_handler(info):
    import sys
    handler_types.append(sys.exc_info()[0])
    events.append(("handler", "Unhandled exception" in info))
    raise RuntimeError("handler failure should be swallowed")

t = scheduler.tasklet()
t.dont_raise = True
t.exception_handler = exception_handler
t.bind(raises_value_error)
t.setup()
scheduler.run()
assert events == ["body_error", ("handler", True)]
assert handler_types == [ValueError]
assert t.alive is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("dont_raise edge handling should match legacy CallableWrapper");
        });
    }

    #[test]
    fn tasklet_switch_pending_kill_and_current_taskletexit_match_legacy_tasklet_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: Tasklet.cpp::Remove, Tasklet.cpp::SwitchImplementation,
# Tasklet.cpp::Kill and test_tasklet.py::{test_remove_and_switch,
# TestKill.test_kill_pending_true,TestKill.test_kill_current}.
values = []
t = scheduler.tasklet(lambda value: values.append(value))(11)
assert t.remove() is t
assert t.alive is True
assert t.paused is True
assert t.scheduled is False
t.switch()
assert values == [11]
assert t.alive is False
assert t.times_switched_to == 1

pending_values = []
pending = scheduler.tasklet(lambda: pending_values.append("ran"))()
pending.kill(pending=True)
assert pending.alive is True
assert pending.scheduled is True
assert scheduler.getruncount() == 2
scheduler.run()
assert pending_values == []
assert pending.alive is False
assert scheduler.getruncount() == 1

caught = []
def kills_current():
    try:
        scheduler.getcurrent().kill()
    except scheduler.TaskletExit:
        caught.append("caught")
        return
    caught.append("missed")

current = scheduler.tasklet(kills_current)()
current.run()
assert caught == ["caught"]
assert current.alive is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("tasklet lifecycle switch/kill behavior should match tested legacy paths");
        });
    }

    #[test]
    fn tasklet_throw_payload_traceback_pending_and_dead_edges_match_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_tasklet.py::TestTaskletThrowBase and
# TestTaskletThrow{Immediate,NonImmediate}. Blocked receive throws preserve
# args/instances/tracebacks; pending throw queues delivery; TaskletExit on
# new/dead tasklets is silent while non-TaskletExit dead throws fail.
import sys
import traceback

events = []

def assert_raises(exc_type, func):
    try:
        func()
    except exc_type:
        return
    raise AssertionError(f"expected {exc_type}")

channel = scheduler.channel()
def receive_index_error():
    try:
        channel.receive()
    except Exception as exc:
        events.append(("args", type(exc), exc.args))

tasklet = scheduler.tasklet(receive_index_error)()
tasklet.run()
assert tasklet.blocked is True
assert channel.balance == -1
tasklet.throw(IndexError, (1, 2, 3))
assert events == [("args", IndexError, (1, 2, 3))]
assert tasklet.alive is False
assert channel.balance == 0

channel = scheduler.channel()
def receive_instance():
    try:
        channel.receive()
    except Exception as exc:
        events.append(("instance", type(exc), exc.args))

tasklet = scheduler.tasklet(receive_instance)()
tasklet.run()
tasklet.throw(IndexError(4, 5, 6))
assert events[-1] == ("instance", IndexError, (4, 5, 6))
assert tasklet.alive is False

channel = scheduler.channel()
def receive_traceback():
    try:
        channel.receive()
    except Exception:
        formatted = "".join(traceback.format_tb(sys.exc_info()[2]))
        events.append(("traceback", "errfunc" in formatted))

tasklet = scheduler.tasklet(receive_traceback)()
tasklet.run()
def errfunc():
    1 / 0
try:
    errfunc()
except Exception:
    tasklet.throw(*sys.exc_info())
assert events[-1] == ("traceback", True)
assert tasklet.alive is False

channel = scheduler.channel()
def receive_pending():
    try:
        channel.receive()
    except ValueError as exc:
        events.append(("pending", exc.args))

tasklet = scheduler.tasklet(receive_pending)()
tasklet.run()
tasklet.throw(ValueError, ("later",), pending=True)
assert tasklet.alive is True
assert tasklet.scheduled is True
scheduler.run()
assert events[-1] == ("pending", ("later",))
assert tasklet.alive is False

new_events = []
new_tasklet = scheduler.tasklet(lambda: new_events.append("ran"))()
new_tasklet.throw(scheduler.TaskletExit)
scheduler.run()
assert new_events == []
assert new_tasklet.alive is False

dead = scheduler.tasklet(lambda: None)()
scheduler.run()
dead.throw(scheduler.TaskletExit)
assert_raises(RuntimeError, lambda: dead.throw(IndexError))

bad = scheduler.tasklet(lambda: None)()
assert_raises(TypeError, lambda: bad.throw(IndexError(1), (1, 2, 3)))
bad.kill()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("tasklet throw behavior should match legacy tasklet throw tests");
        });
    }

    #[test]
    fn tasklet_kill_raise_exception_and_taskletexit_catchability_match_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_tasklet.py::TestKill, TestExceptions, and
# TestTaskletExitException. scheduler.schedule() suspends the tasklet; kill
# and throw/raise_exception resume it with catchable TaskletExit/typed errors.
events = []

def pending_kill_target():
    try:
        scheduler.schedule()
    except scheduler.TaskletExit:
        events.append("pending-kill")
        raise

pending = scheduler.tasklet(pending_kill_target)()
pending.run()
assert pending.alive is True
assert pending.scheduled is True
assert scheduler.getruncount() == 2
pending.kill(pending=True)
assert events == []
assert scheduler.getruncount() == 2
pending.run()
assert events == ["pending-kill"]
assert pending.alive is False
assert scheduler.getruncount() == 1

def immediate_kill_target():
    try:
        scheduler.schedule()
    except scheduler.TaskletExit:
        events.append("immediate-kill")
        raise

immediate = scheduler.tasklet(immediate_kill_target)()
immediate.run()
immediate.kill(pending=False)
assert events[-1] == "immediate-kill"
assert immediate.alive is False
assert scheduler.getruncount() == 1

def raise_exception_target():
    try:
        scheduler.schedule()
    except TypeError:
        events.append("raise-exception")

raised = scheduler.tasklet(raise_exception_target)()
raised.run()
raised.raise_exception(TypeError)
assert events[-1] == "raise-exception"
assert raised.alive is False

def throw_target():
    try:
        scheduler.schedule()
    except TypeError:
        events.append("throw")

thrown = scheduler.tasklet(throw_target)()
thrown.run()
thrown.throw(TypeError)
assert events[-1] == "throw"
assert thrown.alive is False

def cannot_catch_taskletexit_with_exception():
    try:
        scheduler.schedule()
    except Exception:
        events.append("caught-by-exception")

tasklet_exit = scheduler.tasklet(cannot_catch_taskletexit_with_exception)()
tasklet_exit.run()
tasklet_exit.kill()
assert "caught-by-exception" not in events
assert tasklet_exit.alive is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("kill and exception injection should match legacy tasklet tests");
        });
    }

    #[test]
    fn tasklet_throw_and_kill_wrong_thread_errors_match_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_tasklet.py::test_kill_from_another_thread and
# Tasklet.cpp::{Kill,ThrowException} owner-thread checks.
import threading

tasklet = scheduler.tasklet(lambda: None)()
errors = []

def worker():
    for operation in (lambda: tasklet.throw(IndexError), lambda: tasklet.kill()):
        try:
            operation()
        except RuntimeError as exc:
            errors.append(str(exc))

thread = threading.Thread(target=worker)
thread.start()
thread.join()

assert any("Cannot throw tasklet from another thread" in error for error in errors), errors
assert any("Cannot kill tasklet from another thread" in error for error in errors), errors
assert tasklet.alive is True
assert tasklet.scheduled is True
tasklet.kill()
scheduler.run()
assert tasklet.alive is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("wrong-thread throw and kill should match legacy tasklet errors");
        });
    }

    #[test]
    fn tasklet_cross_thread_operations_match_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_tasklet.py::TestTasklets.
# {test_insert_from_another_thread,test_remove_from_another_thread,
# test_run_from_another_thread,test_switch_from_another_thread,
# test_kill_from_another_thread,test_bind_from_another_thread,
# test_setup_from_another_thread}. Foreign insert/remove use the tasklet's
# owner queue; foreign run is ignored; switch/kill/bind/setup raise the
# legacy RuntimeError text.
import threading

def join_worker(target):
    errors = []
    def wrapped():
        try:
            target()
        except BaseException as exc:
            errors.append(repr(exc))
    thread = threading.Thread(target=wrapped)
    thread.start()
    thread.join()
    assert errors == [], errors

def assert_runtime(callable_, message):
    try:
        callable_()
    except RuntimeError as exc:
        assert str(exc) == message, str(exc)
    else:
        raise AssertionError("expected RuntimeError")

value_in = "TEST_VALUE"

value_out = []
def record_insert(value):
    value_out.append(value)

inserted = scheduler.tasklet(record_insert)
inserted.bind(record_insert)
inserted.setup(value_in)
def worker_insert():
    inserted.insert()
    assert scheduler.getruncount() == 1
join_worker(worker_insert)
assert scheduler.getruncount() == 2
scheduler.run()
assert value_out == [value_in]

value_out = []
removed = scheduler.tasklet(lambda value: value_out.append(value))(value_in)
join_worker(lambda: removed.remove())
assert scheduler.getruncount() == 1
scheduler.run()
assert value_out == []
removed.kill()

value_out = []
foreign_run = scheduler.tasklet(lambda value: value_out.append(value))(value_in)
def worker_run():
    foreign_run.run()
    assert scheduler.getruncount() == 1
join_worker(worker_run)
assert scheduler.getruncount() == 2
scheduler.run()
assert value_out == [value_in]

value_out = []
switched = scheduler.tasklet(lambda value: value_out.append(value))(value_in)
def worker_switch():
    assert_runtime(
        switched.switch,
        "Failed to switch tasklet: Cannot switch tasklet from another thread",
    )
    assert scheduler.getruncount() == 1
join_worker(worker_switch)
assert scheduler.getruncount() == 2
scheduler.run()
assert value_out == [value_in]

value_out = []
killed = scheduler.tasklet(lambda value: value_out.append(value))(value_in)
def worker_kill():
    assert_runtime(
        killed.kill,
        "Failed to kill tasklet: Cannot kill tasklet from another thread",
    )
    assert scheduler.getruncount() == 1
join_worker(worker_kill)
assert scheduler.getruncount() == 2
scheduler.run()
assert value_out == [value_in]

value_out = []
bound = scheduler.tasklet(lambda value: value_out.append(value))
def worker_bind():
    assert_runtime(
        bound.bind,
        "Failed to unbind tasklet: Cannot unbind tasklet from another thread",
    )
    assert scheduler.getruncount() == 1
join_worker(worker_bind)
bound.bind(lambda value: value_out.append(value))
bound.setup(value_in)
assert scheduler.getruncount() == 2
scheduler.run()
assert value_out == [value_in]

value_out = []
setup = scheduler.tasklet(lambda value: value_out.append(value))
setup.bind(lambda value: value_out.append(value))
def worker_setup():
    assert_runtime(
        setup.setup,
        "Failed to setup tasklet: Cannot setup tasklet from another thread",
    )
    assert scheduler.getruncount() == 1
join_worker(worker_setup)
setup.setup(value_in)
assert scheduler.getruncount() == 2
scheduler.run()
assert value_out == [value_in]
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("legacy cross-thread tasklet operation parity should hold");
        });
    }

    #[test]
    fn tasklet_thread_exit_cleanup_matches_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_tasklet.py::TestTasklets.
# {test_new_tasklets_cleanup_on_thread_finish,
# test_partially_complete_tasklets_cleanup_on_thread_finish}.
import gc
import sys
import threading

for _ in range(3):
    gc.collect()
active_baseline = scheduler.get_active_tasklet_count()

new_tasklet = [None]
def new_thread_func():
    new_tasklet[0] = scheduler.tasklet(lambda: None)()

thread = threading.Thread(target=new_thread_func)
thread.start()
thread.join()
assert sys.getrefcount(new_tasklet[0]) == 2, sys.getrefcount(new_tasklet[0])
new_tasklet[0] = None
for _ in range(3):
    gc.collect()
assert scheduler.get_active_tasklet_count() <= active_baseline

partial_tasklet = [None]
test_value = [0]
def partial_callable():
    try:
        scheduler.schedule()
        test_value[0] = 1
    except scheduler.TaskletExit:
        test_value[0] = 2

def partial_thread_func():
    partial_tasklet[0] = scheduler.tasklet(partial_callable)()
    partial_tasklet[0].run()

thread = threading.Thread(target=partial_thread_func)
thread.start()
thread.join()
assert test_value[0] == 2, test_value
assert sys.getrefcount(partial_tasklet[0]) == 2, sys.getrefcount(partial_tasklet[0])
partial_tasklet[0] = None
for _ in range(3):
    gc.collect()
assert scheduler.get_active_tasklet_count() <= active_baseline
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("thread-exit tasklet cleanup should match legacy tests");
        });
    }

    #[test]
    fn channel_inter_thread_and_thread_exit_cleanup_match_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_channel.py::TestChannels.
# {test_inter_thread_communication,test_tasklet_channel_cleanup_on_thread_finish}.
import gc
import sys
import threading
import time

command_channel = scheduler.channel()
events = []

def master_func():
    command_channel.send("ECHO 1")
    command_channel.send("ECHO 2")
    command_channel.send("ECHO 3")
    command_channel.send("QUIT")

def slave_func():
    while True:
        command = command_channel.receive()
        events.append(command)
        if command == "QUIT":
            break

def scheduler_run(tasklet_func):
    tasklet = scheduler.tasklet(tasklet_func)()
    for _ in range(1000):
        if not tasklet.alive:
            return
        scheduler.run()
        time.sleep(0)
    raise AssertionError("scheduler thread did not finish")

thread_errors = []
def run_master():
    try:
        scheduler_run(master_func)
    except BaseException as exc:
        thread_errors.append(repr(exc))

thread = threading.Thread(target=run_master)
thread.start()
scheduler_run(slave_func)
thread.join(5)
assert not thread.is_alive()
assert thread_errors == [], thread_errors
assert events == ["ECHO 1", "ECHO 2", "ECHO 3", "QUIT"], events
assert command_channel.balance == 0

for _ in range(3):
    gc.collect()
active_baseline = scheduler.get_active_tasklet_count()

cleanup_channel = scheduler.channel()
blocked_tasklet = [None]
test_value = [False]

def blocked_callable():
    cleanup_channel.receive()
    test_value[0] = True

def blocked_thread_func():
    blocked_tasklet[0] = scheduler.tasklet(blocked_callable)()
    blocked_tasklet[0].run()

thread = threading.Thread(target=blocked_thread_func)
thread.start()
thread.join()
assert test_value[0] is False
assert cleanup_channel.balance == 0, cleanup_channel.balance
assert sys.getrefcount(blocked_tasklet[0]) == 2, sys.getrefcount(blocked_tasklet[0])
blocked_tasklet[0] = None
for _ in range(3):
    gc.collect()
assert scheduler.get_active_tasklet_count() <= active_baseline
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("channel inter-thread and cleanup parity should hold");
        });
    }

    #[test]
    fn c_api_tasklet_new_setup_check_alive_and_block_trap_match_legacy_capi_tests() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let new_tasklet: CApiPyObjectPyTypeObject =
                c_api_fn(api.py_tasklet_new, "PyTasklet_New");
            let setup: CApiIntObjectObjectObject =
                c_api_fn(api.py_tasklet_setup, "PyTasklet_Setup");
            let check: CApiIntObject = c_api_fn(api.py_tasklet_check, "PyTasklet_Check");
            let alive: CApiIntObject = c_api_fn(api.py_tasklet_alive, "PyTasklet_Alive");
            let get_block_trap: CApiIntObject =
                c_api_fn(api.py_tasklet_get_block_trap, "PyTasklet_GetBlockTrap");
            let set_block_trap: CApiVoidObjectInt =
                c_api_fn(api.py_tasklet_set_block_trap, "PyTasklet_SetBlockTrap");
            let run_n: CApiPyObjectInt =
                c_api_fn(api.py_scheduler_run_n_tasklets, "PyScheduler_RunNTasklets");

            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            py.run_bound(
                r#"
# Parity source: capiTest/Tasklet.cpp::{PyTasklet_New,
# PyTasklet_Setup,PyTasklet_Check,PyTasklet_GetBlockTrap,
# PyTasklet_Alive}; Scheduler.h tasklet C API
# slots; SchedulerModule.cpp C API wrappers; PyTasklet.cpp::TaskletSetup;
# and Tasklet.cpp::{Setup,Bind,Insert,IsAlive,IsBlocktrapped}.
values = []
def record(value):
    values.append(value)
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            let callable = locals.get_item("record").unwrap().unwrap();
            let constructor_args = PyTuple::new_bound(py, [callable]).to_object(py);
            let tasklet_ptr = new_tasklet(api.py_tasklet_type, constructor_args.as_ptr());
            assert!(!tasklet_ptr.is_null());
            let tasklet = unsafe { PyObject::from_owned_ptr(py, tasklet_ptr) };

            assert_eq!(check(tasklet.as_ptr()), 1);
            assert_eq!(check(ptr::null_mut()), 0);
            assert_eq!(check(module.as_ptr()), 0);
            assert_eq!(alive(tasklet.as_ptr()), 0);
            assert_eq!(get_block_trap(tasklet.as_ptr()), 0);
            set_block_trap(tasklet.as_ptr(), 1);
            assert_eq!(get_block_trap(tasklet.as_ptr()), 1);
            assert!(tasklet
                .bind(py)
                .getattr("block_trap")
                .unwrap()
                .is_truthy()
                .unwrap());
            set_block_trap(tasklet.as_ptr(), 0);
            assert_eq!(get_block_trap(tasklet.as_ptr()), 0);

            let setup_args = PyTuple::new_bound(py, [101i64]).to_object(py);
            assert_eq!(
                setup(tasklet.as_ptr(), setup_args.as_ptr(), ptr::null_mut()),
                0
            );
            assert_eq!(alive(tasklet.as_ptr()), 1);
            assert_eq!(run_count(&module), 2);

            let duplicate_setup_args = PyTuple::new_bound(py, [202i64]).to_object(py);
            assert_eq!(
                setup(
                    tasklet.as_ptr(),
                    duplicate_setup_args.as_ptr(),
                    ptr::null_mut()
                ),
                -1
            );
            let error = PyErr::fetch(py);
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert!(error.to_string().contains("already scheduled"));

            let run_result = run_n(1);
            assert!(!run_result.is_null());
            unsafe {
                ffi::Py_DECREF(run_result);
            }
            py.run_bound(
                r#"
assert values == [101]
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(alive(tasklet.as_ptr()), 0);
        });
    }

    #[test]
    fn c_api_tasklet_setup_reference_counts_match_legacy_capi_test() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let new_tasklet: CApiPyObjectPyTypeObject =
                c_api_fn(api.py_tasklet_new, "PyTasklet_New");
            let setup: CApiIntObjectObjectObject =
                c_api_fn(api.py_tasklet_setup, "PyTasklet_Setup");
            let kill: CApiIntObject = c_api_fn(api.py_tasklet_kill, "PyTasklet_Kill");

            let locals = PyDict::new_bound(py);
            py.run_bound(
                r#"
# Parity source: capiTest/Tasklet.cpp::PyTasklet_Setup_ReferenceCount;
# PythonCppType.cpp::{Incref,Decref,ReferenceCount}; and
# SchedulerModule.cpp::PyTasklet_Setup/PyTasklet_Kill. Scheduling keeps one
# queue reference, duplicate setup does not leak another, and kill releases it.
def noop():
    pass
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            let callable = locals.get_item("noop").unwrap().unwrap();
            let constructor_args = PyTuple::new_bound(py, [callable]).to_object(py);
            let tasklet_ptr = new_tasklet(api.py_tasklet_type, constructor_args.as_ptr());
            assert!(!tasklet_ptr.is_null());
            let tasklet = unsafe { PyObject::from_owned_ptr(py, tasklet_ptr) };
            let callable_args = PyTuple::empty_bound(py).to_object(py);

            assert_eq!(unsafe { ffi::Py_REFCNT(tasklet.as_ptr()) }, 1);
            assert_eq!(
                setup(tasklet.as_ptr(), callable_args.as_ptr(), ptr::null_mut()),
                0
            );
            assert_eq!(unsafe { ffi::Py_REFCNT(tasklet.as_ptr()) }, 2);

            assert_eq!(
                setup(tasklet.as_ptr(), callable_args.as_ptr(), ptr::null_mut()),
                -1
            );
            let error = PyErr::fetch(py);
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert!(error.to_string().contains("already scheduled"));
            assert_eq!(unsafe { ffi::Py_REFCNT(tasklet.as_ptr()) }, 2);

            assert_eq!(kill(tasklet.as_ptr()), 0);
            assert_eq!(unsafe { ffi::Py_REFCNT(tasklet.as_ptr()) }, 1);
        });
    }

    #[test]
    fn c_api_tasklet_is_main_times_and_context_match_legacy_capi_entries() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let check: CApiIntObject = c_api_fn(api.py_tasklet_check, "PyTasklet_Check");
            let alive: CApiIntObject = c_api_fn(api.py_tasklet_alive, "PyTasklet_Alive");
            let is_main: CApiIntObject = c_api_fn(api.py_tasklet_is_main, "PyTasklet_IsMain");
            let get_current: CApiPyObject =
                c_api_fn(api.py_scheduler_get_current, "PyScheduler_GetCurrent");
            let get_times_switched_to: CApiLongObject = c_api_fn(
                api.py_tasklet_get_times_switched_to,
                "PyTasklet_GetTimesSwitchedTo",
            );
            let get_context: CApiCStringObject =
                c_api_fn(api.py_tasklet_get_context, "PyTasklet_GetContext");
            let run_n: CApiPyObjectInt =
                c_api_fn(api.py_scheduler_run_n_tasklets, "PyScheduler_RunNTasklets");

            // Parity source: capiTest/Tasklet.cpp::{PyTasklet_IsMain,
            // PyTasklet_Alive}; Scheduler.h late tasklet C API slots;
            // SchedulerModule.cpp::{PyTasklet_GetTimesSwitchedTo,
            // PyTasklet_GetContext}; PyTasklet.cpp::{TaskletIsMainGet,
            // TaskletTimesSwitchedToGet,TaskletContextGet}; and
            // Tasklet.cpp::{IsMain,GetTimesSwitchedTo,GetContext}.
            let main_ptr = get_current();
            assert!(!main_ptr.is_null());
            let main = unsafe { PyObject::from_owned_ptr(py, main_ptr) };
            assert_eq!(check(main.as_ptr()), 1);
            assert_eq!(is_main(main.as_ptr()), 1);
            assert_eq!(alive(main.as_ptr()), 1);

            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            py.run_bound(
                r#"
values = []
def record():
    values.append("ran")

tasklet = scheduler.tasklet(record)
tasklet.context = "legacy-context"
tasklet()
assert tasklet.alive is True
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let tasklet = locals.get_item("tasklet").unwrap().unwrap();
            assert_eq!(is_main(tasklet.as_ptr()), 0);
            assert_eq!(alive(tasklet.as_ptr()), 1);
            assert_eq!(get_times_switched_to(tasklet.as_ptr()), 0);

            let context = get_context(tasklet.as_ptr());
            assert!(!context.is_null());
            let context = unsafe { std::ffi::CStr::from_ptr(context) };
            assert_eq!(context.to_str().unwrap(), "legacy-context");

            let run_result = run_n(1);
            assert!(!run_result.is_null());
            unsafe {
                ffi::Py_DECREF(run_result);
            }
            py.run_bound(
                r#"
assert values == ["ran"]
assert tasklet.alive is False
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(alive(tasklet.as_ptr()), 0);
            assert_eq!(get_times_switched_to(tasklet.as_ptr()), 1);
        });
    }

    #[test]
    fn c_api_tasklet_insert_and_kill_match_legacy_capi_tasklet_tests() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let insert: CApiIntObject = c_api_fn(api.py_tasklet_insert, "PyTasklet_Insert");
            let kill: CApiIntObject = c_api_fn(api.py_tasklet_kill, "PyTasklet_Kill");
            let run_n: CApiPyObjectInt =
                c_api_fn(api.py_scheduler_run_n_tasklets, "PyScheduler_RunNTasklets");

            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            py.run_bound(
                r#"
# Parity source: capiTest/Tasklet.cpp::PyTasklet_Insert and
# capiTest/Tasklet.cpp::PyTasklet_Kill. The legacy insert path rejects
# blocked/dead tasklets in Tasklet.cpp::Tasklet::Insert, and the C API kill
# removes a scheduled tasklet while returning 0.
values = []
def record(value):
    values.append(value)

tasklet = scheduler.tasklet(record)
tasklet.bind(record, (7,))
assert tasklet.alive is True
assert tasklet.scheduled is False
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            let tasklet = locals.get_item("tasklet").unwrap().unwrap();
            assert_eq!(insert(tasklet.as_ptr()), 0);
            assert_eq!(run_count(&module), 2);
            let run_result = run_n(1);
            assert!(!run_result.is_null());
            let run_result = unsafe { PyObject::from_owned_ptr(py, run_result) };
            assert!(run_result.bind(py).is_none());
            py.run_bound(
                r#"
assert values == [7]
assert tasklet.alive is False
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            assert_eq!(insert(tasklet.as_ptr()), -1);
            let error = PyErr::fetch(py);
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            assert!(error.to_string().contains("dead tasklet"));

            py.run_bound(
                r#"
kill_values = []
kill_tasklet = scheduler.tasklet(lambda: kill_values.append("ran"))()
assert scheduler.getruncount() == 2
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let kill_tasklet = locals.get_item("kill_tasklet").unwrap().unwrap();
            assert_eq!(kill(kill_tasklet.as_ptr()), 0);
            py.run_bound(
                r#"
assert scheduler.getruncount() == 1
assert kill_tasklet.alive is False
scheduler.run()
assert kill_values == []
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn c_api_tasklet_insert_schedule_remove_continuation_resumes_like_legacy() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let insert: CApiIntObject = c_api_fn(api.py_tasklet_insert, "PyTasklet_Insert");
            let run_n: CApiPyObjectInt =
                c_api_fn(api.py_scheduler_run_n_tasklets, "PyScheduler_RunNTasklets");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            py.run_bound(
                r#"
# Parity source: capiTest/Tasklet.cpp::PyTasklet_Insert. A tasklet paused by
# scheduler.schedule_remove() resumes after PyTasklet_Insert + scheduler run.
values = []
def pauses():
    scheduler.schedule_remove()
    values.append("resumed")

tasklet = scheduler.tasklet(pauses)()
scheduler.run()
assert values == []
assert tasklet.alive is True
assert tasklet.paused is True
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            let tasklet = locals.get_item("tasklet").unwrap().unwrap();
            assert_eq!(insert(tasklet.as_ptr()), 0);
            let run_result = run_n(1);
            assert!(!run_result.is_null());
            unsafe {
                ffi::Py_DECREF(run_result);
            }
            py.run_bound(
                r#"
assert values == ["resumed"]
assert tasklet.alive is False
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn python_greenlet_continuations_resume_scheduler_and_channel_yields() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            py.run_bound(
                r#"
# Parity sources:
# - test_scheduler.py::TestSchedule.test_schedule
# - test_scheduler.py::TestSchedule.test_schedule_remove_fail
# - capiTest/Tasklet.cpp::PyTasklet_Insert
# - test_channel.py::TestChannels.{test_blocking_send,test_blocking_receive}
schedule_events = []
def scheduled_yielder():
    schedule_events.append("before")
    scheduler.schedule()
    schedule_events.append("after")

def scheduled_peer():
    schedule_events.append("peer")

scheduler.tasklet(scheduled_yielder)()
scheduler.tasklet(scheduled_peer)()
scheduler.run()
assert schedule_events == ["before", "peer", "after"], schedule_events
assert scheduler.getruncount() == 1

remove_events = []
def removable():
    remove_events.append("before")
    scheduler.schedule_remove()
    remove_events.append("after")

removed = scheduler.tasklet(removable)()
removed.run()
assert remove_events == ["before"], remove_events
assert removed.alive is True
assert removed.paused is True
removed.run()
assert remove_events == ["before", "after"], remove_events
assert removed.alive is False
assert scheduler.getruncount() == 1

schedule_remove_fail_events = []
def nested_schedule_remove():
    def foo(previous):
        schedule_remove_fail_events.append("foo")
        assert previous.scheduled is False
        previous.insert()
        assert previous.scheduled is True

    t = scheduler.tasklet(foo)(scheduler.getcurrent())
    assert scheduler.getruncount() == 3
    scheduler.schedule_remove()
    assert scheduler.getruncount() == 2
    assert schedule_remove_fail_events == ["foo"], schedule_remove_fail_events

nested = scheduler.tasklet(nested_schedule_remove)()
nested.run()
scheduler.run()
assert scheduler.getruncount() == 1

receive_events = []
receive_channel = scheduler.channel()
def receiver():
    receive_events.append(("received", receive_channel.receive()))
    receive_events.append("after_receive")

receiving_tasklet = scheduler.tasklet(receiver)()
receiving_tasklet.run()
assert receiving_tasklet.blocked is True
receive_channel.send("payload")
scheduler.run()
assert receive_events == [("received", "payload"), "after_receive"], receive_events
assert receiving_tasklet.alive is False
assert receive_channel.balance == 0

send_events = []
send_channel = scheduler.channel()
def sender():
    send_channel.send("payload")
    send_events.append("after_send")

sending_tasklet = scheduler.tasklet(sender)()
sending_tasklet.run()
assert sending_tasklet.blocked is True
assert send_channel.receive() == "payload"
scheduler.run()
assert send_events == ["after_send"], send_events
assert sending_tasklet.alive is False
assert send_channel.balance == 0
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("greenlet-backed continuations should resume after scheduler/channel yields");
        });
    }

    #[test]
    fn c_api_channel_constructor_introspection_entries_match_legacy_capi_paths() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let new_channel: CApiPyObjectPyType = c_api_fn(api.py_channel_new, "PyChannel_New");
            let check: CApiIntObject = c_api_fn(api.py_channel_check, "PyChannel_Check");
            let get_queue: CApiPyObjectObject =
                c_api_fn(api.py_channel_get_queue, "PyChannel_GetQueue");
            let get_preference: CApiIntObject =
                c_api_fn(api.py_channel_get_preference, "PyChannel_GetPreference");
            let set_preference: CApiVoidObjectInt =
                c_api_fn(api.py_channel_set_preference, "PyChannel_SetPreference");
            let get_balance: CApiIntObject =
                c_api_fn(api.py_channel_get_balance, "PyChannel_GetBalance");

            // Parity source: capiTest/Channel.cpp::{PyChannel_New,
            // PyChannel_GetQueue,PyChannel_GetPreference,PyChannel_SetPreference,
            // PyChannel_GetBalance,PyChannel_Check}; Scheduler.h channel function
            // pointer layout; and SchedulerModule.cpp C API wrappers delegating to
            // PyChannel.cpp::ChannelQueueGet and Channel.cpp::BlockedQueueFront.
            let channel_ptr = new_channel(api.py_channel_type);
            assert!(!channel_ptr.is_null());
            let channel = unsafe { PyObject::from_owned_ptr(py, channel_ptr) };
            assert_eq!(check(channel.as_ptr()), 1);
            assert_eq!(check(ptr::null_mut()), 0);
            assert_eq!(check(module.as_ptr()), 0);

            assert_eq!(get_preference(channel.as_ptr()), -1);
            set_preference(channel.as_ptr(), 0);
            assert_eq!(get_preference(channel.as_ptr()), 0);
            set_preference(channel.as_ptr(), 1);
            assert_eq!(get_preference(channel.as_ptr()), 1);
            set_preference(channel.as_ptr(), -2);
            assert_eq!(get_preference(channel.as_ptr()), -1);
            set_preference(channel.as_ptr(), 2);
            assert_eq!(get_preference(channel.as_ptr()), 1);

            assert_eq!(get_balance(channel.as_ptr()), 0);
            let empty_queue = get_queue(channel.as_ptr());
            assert!(!empty_queue.is_null());
            let empty_queue = unsafe { PyObject::from_owned_ptr(py, empty_queue) };
            assert!(empty_queue.bind(py).is_none());

            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            locals.set_item("channel", channel.clone_ref(py)).unwrap();
            py.run_bound(
                r#"
def sender():
    channel.send(101)

send_tasklet = scheduler.tasklet(sender)()
scheduler.run()
assert channel.balance == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(get_balance(channel.as_ptr()), 1);
            let send_tasklet = locals.get_item("send_tasklet").unwrap().unwrap();
            let queue = get_queue(channel.as_ptr());
            assert_eq!(queue, send_tasklet.as_ptr());
            unsafe {
                ffi::Py_DECREF(queue);
            }
            py.run_bound(
                r#"
assert channel.receive() == 101
assert channel.balance == 0
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(get_balance(channel.as_ptr()), 0);

            py.run_bound(
                r#"
receive_channel = scheduler.channel()
def receiver():
    receive_channel.receive()

receive_tasklet = scheduler.tasklet(receiver)()
scheduler.run()
assert receive_channel.balance == -1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let receive_channel = locals.get_item("receive_channel").unwrap().unwrap();
            assert_eq!(get_balance(receive_channel.as_ptr()), -1);
            let receive_tasklet = locals.get_item("receive_tasklet").unwrap().unwrap();
            let queue = get_queue(receive_channel.as_ptr());
            assert_eq!(queue, receive_tasklet.as_ptr());
            unsafe {
                ffi::Py_DECREF(queue);
            }
            py.run_bound(
                r#"
receive_channel.clear()
assert receive_channel.balance == 0
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let empty_queue = get_queue(receive_channel.as_ptr());
            assert!(!empty_queue.is_null());
            let empty_queue = unsafe { PyObject::from_owned_ptr(py, empty_queue) };
            assert!(empty_queue.bind(py).is_none());

            let default_type_channel = new_channel(ptr::null_mut());
            assert!(!default_type_channel.is_null());
            assert_eq!(check(default_type_channel), 1);
            unsafe {
                ffi::Py_DECREF(default_type_channel);
            }
        });
    }

    #[test]
    fn c_api_channel_send_receive_and_exception_entries_match_safe_legacy_capi_paths() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let send: CApiIntObjectObject = c_api_fn(api.py_channel_send, "PyChannel_Send");
            let _receive: CApiPyObjectObject =
                c_api_fn(api.py_channel_receive, "PyChannel_Receive");
            let send_exception: CApiIntObjectObjectObject =
                c_api_fn(api.py_channel_send_exception, "PyChannel_SendException");
            let send_throw: CApiIntObjectObjectObjectObject =
                c_api_fn(api.py_channel_send_throw, "PyChannel_SendThrow");

            // Parity source: capiTest/Channel.cpp::{PyChannel_Send,
            // PyChannel_Receive,PyChannel_SendException,PyChannel_SendThrow}.
            // This covers value/error transfer once a receiver is already
            // waiting; the receiver resumes and observes the payload.
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            py.run_bound(
                r#"
received_values = []
channel = scheduler.channel()
receiver = scheduler.tasklet(lambda: received_values.append(channel.receive()))()
scheduler.run_n_tasklets(1)
assert channel.balance == -1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let channel = locals.get_item("channel").unwrap().unwrap();
            let value = 101i64.into_py(py);
            assert_eq!(send(channel.as_ptr(), value.as_ptr()), 0);
            py.run_bound(
                r#"
scheduler.run()
assert received_values == [101]
assert receiver.alive is False
assert channel.balance == 0
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            py.run_bound(
                r#"
import sys
import traceback
exception_events = []
exception_channel = scheduler.channel()
def receive_exception():
    try:
        exception_channel.receive()
    except ValueError as exc:
        exc_type, exc_value, exc_tb = sys.exc_info()
        exception_events.append((
            exc_type is ValueError,
            exc_value is exc,
            exc.args,
            traceback.extract_tb(exc_tb)[-1][2],
        ))

exception_receiver = scheduler.tasklet(receive_exception)()
scheduler.run_n_tasklets(1)
assert exception_channel.balance == -1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let exception_channel = locals.get_item("exception_channel").unwrap().unwrap();
            let value_error = py.get_type_bound::<PyValueError>();
            let args = PyTuple::new_bound(py, [1, 2, 3]).to_object(py);
            assert_eq!(
                send_exception(
                    exception_channel.as_ptr(),
                    value_error.as_ptr(),
                    args.as_ptr()
                ),
                0
            );
            py.run_bound(
                r#"
scheduler.run()
assert exception_events == [(True, True, (1, 2, 3), "receive_exception")], exception_events
assert exception_receiver.alive is False
assert exception_channel.balance == 0
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            py.run_bound(
                r#"
throw_events = []
throw_channel = scheduler.channel()
def c_api_bar():
    raise ValueError(1, 2, 3)

def receive_throw():
    try:
        throw_channel.receive()
    except ValueError as exc:
        exc_type, exc_value, exc_tb = sys.exc_info()
        throw_events.append((
            exc_type is ValueError,
            exc_value is exc,
            exc.args,
            traceback.extract_tb(exc_tb)[-1][2],
        ))

throw_receiver = scheduler.tasklet(receive_throw)()
scheduler.run_n_tasklets(1)
try:
    c_api_bar()
except ValueError:
    import sys
    throw_exc_type, throw_exc_value, throw_exc_tb = sys.exc_info()
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let throw_channel = locals.get_item("throw_channel").unwrap().unwrap();
            let throw_exc_type = locals.get_item("throw_exc_type").unwrap().unwrap();
            let throw_exc_value = locals.get_item("throw_exc_value").unwrap().unwrap();
            let throw_exc_tb = locals.get_item("throw_exc_tb").unwrap().unwrap();
            assert_eq!(
                send_throw(
                    throw_channel.as_ptr(),
                    throw_exc_type.as_ptr(),
                    throw_exc_value.as_ptr(),
                    throw_exc_tb.as_ptr(),
                ),
                0
            );
            py.run_bound(
                r#"
scheduler.run()
assert throw_events == [(True, True, (1, 2, 3), "c_api_bar")], throw_events
assert throw_receiver.alive is False
assert throw_channel.balance == 0
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            py.run_bound(
                r#"
throw_no_value_events = []
throw_no_value_channel = scheduler.channel()
def receive_throw_no_value():
    try:
        throw_no_value_channel.receive()
    except ValueError as exc:
        exc_type, exc_value, exc_tb = sys.exc_info()
        throw_no_value_events.append((
            exc_type is ValueError,
            exc_value is exc,
            exc.args,
            traceback.extract_tb(exc_tb)[-1][2],
        ))

throw_no_value_receiver = scheduler.tasklet(receive_throw_no_value)()
scheduler.run_n_tasklets(1)
assert throw_no_value_channel.balance == -1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let throw_no_value_channel =
                locals.get_item("throw_no_value_channel").unwrap().unwrap();
            assert_eq!(
                send_throw(
                    throw_no_value_channel.as_ptr(),
                    value_error.as_ptr(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                0
            );
            py.run_bound(
                r#"
scheduler.run()
assert throw_no_value_events == [(True, True, (), "receive_throw_no_value")], throw_no_value_events
assert throw_no_value_receiver.alive is False
assert throw_no_value_channel.balance == 0
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn c_api_schedulertest_channel_send_ref_cleanup_matches_legacy_fixture() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let api = scheduler_c_api_import_for_test(py);
            let send: CApiIntObjectObject = c_api_fn(api.py_channel_send, "PyChannel_Send");

            // Parity source:
            // capiTest/InterpreterWithSchedulerModule.cpp::
            // schedulertest_channel_send validates PyChannel_Check, INCREFs
            // the payload before PyChannel_Send, DECREFs it afterwards, and
            // returns None when the C API call reports success. Scheduler.h
            // exposes that slot, and SchedulerModule.cpp::PyChannel_Send takes
            // a borrowed PyObject* from C callers.
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();
            py.run_bound(
                r#"
import gc
import sys

class Payload:
    pass

received_values = []
channel = scheduler.channel()
value = Payload()
receiver = scheduler.tasklet(lambda: received_values.append(channel.receive()))()
scheduler.run_n_tasklets(1)
assert channel.balance == -1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            let channel = locals.get_item("channel").unwrap().unwrap();
            let value = locals.get_item("value").unwrap().unwrap();
            let baseline_refcount: isize = py
                .eval_bound("sys.getrefcount(value)", None, Some(&locals))
                .unwrap()
                .extract()
                .unwrap();
            locals
                .set_item("baseline_refcount", baseline_refcount)
                .unwrap();

            unsafe {
                ffi::Py_INCREF(value.as_ptr());
            }
            let send_result = send(channel.as_ptr(), value.as_ptr());
            unsafe {
                ffi::Py_DECREF(value.as_ptr());
            }
            assert_eq!(send_result, 0);

            py.run_bound(
                r#"
scheduler.run()
assert received_values == [value]
del received_values[:]
for _ in range(3):
    gc.collect()
assert sys.getrefcount(value) == baseline_refcount, (
    sys.getrefcount(value),
    baseline_refcount,
)
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("schedulertest-style send should not leak a payload reference");
        });
    }

    #[test]
    fn c_api_scheduler_identity_run_count_and_run_n_tasklets_match_legacy_capi_paths() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let get_scheduler: CApiPyObject =
                c_api_fn(api.py_scheduler_get_scheduler, "PyScheduler_GetScheduler");
            let get_run_count: CApiInt =
                c_api_fn(api.py_scheduler_get_run_count, "PyScheduler_GetRunCount");
            let get_current: CApiPyObject =
                c_api_fn(api.py_scheduler_get_current, "PyScheduler_GetCurrent");
            let run_n_tasklets: CApiPyObjectInt =
                c_api_fn(api.py_scheduler_run_n_tasklets, "PyScheduler_RunNTasklets");
            let tasklet_check: CApiIntObject = c_api_fn(api.py_tasklet_check, "PyTasklet_Check");
            let tasklet_is_main: CApiIntObject =
                c_api_fn(api.py_tasklet_is_main, "PyTasklet_IsMain");

            // Parity source:
            // capiTest/InterpreterWithSchedulerModule.cpp::SetUp calls
            // PyScheduler_GetScheduler, and capiTest/Scheduler.cpp covers
            // PyScheduler_GetRunCount, PyScheduler_GetCurrent, and
            // PyScheduler_RunNTasklets against SchedulerModule.cpp C API
            // wrappers delegating to ScheduleManager.cpp::{GetCachedTaskletCount,
            // GetCurrentTasklet,RunNTasklets}.
            let scheduler_object = get_scheduler();
            assert!(!scheduler_object.is_null());
            let scheduler_object = unsafe { PyObject::from_owned_ptr(py, scheduler_object) };
            assert!(scheduler_object
                .bind(py)
                .downcast::<ScheduleManager>()
                .is_ok());

            assert_eq!(get_run_count(), 1);
            let current = get_current();
            assert!(!current.is_null());
            assert_eq!(tasklet_check(current), 1);
            assert_eq!(tasklet_is_main(current), 1);
            unsafe {
                ffi::Py_DECREF(current);
            }

            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            py.run_bound(
                r#"
run_values = []
def record(value):
    run_values.append(value)

scheduler.tasklet(record)(1)
scheduler.tasklet(record)(2)
scheduler.tasklet(record)(3)
assert scheduler.getruncount() == 4
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(get_run_count(), 4);

            for expected_count in [3, 2, 1] {
                let result = run_n_tasklets(1);
                assert!(!result.is_null());
                let result = unsafe { PyObject::from_owned_ptr(py, result) };
                assert!(result.bind(py).is_none());
                assert_eq!(get_run_count(), expected_count);
            }

            py.run_bound(
                r#"
assert run_values == [1, 2, 3]
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn c_api_scheduler_tasklet_lifetime_counts_match_legacy_capi_paths() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let get_scheduler: CApiPyObject =
                c_api_fn(api.py_scheduler_get_scheduler, "PyScheduler_GetScheduler");
            let new_tasklet: CApiPyObjectPyTypeObject =
                c_api_fn(api.py_tasklet_new, "PyTasklet_New");
            let tasklet_check: CApiIntObject = c_api_fn(api.py_tasklet_check, "PyTasklet_Check");
            let all_time_tasklets: CApiInt = c_api_fn(
                api.py_scheduler_get_all_time_tasklet_count,
                "PyScheduler_GetAllTimeTaskletCount",
            );
            let active_tasklets: CApiInt = c_api_fn(
                api.py_scheduler_get_active_tasklet_count,
                "PyScheduler_GetActiveTaskletCount",
            );

            // Parity source: capiTest/Scheduler.cpp::{PyScheduler_GetAllTimeTaskletCount,
            // PyScheduler_GetActiveTaskletCount} creates tasklets through
            // InterpreterWithSchedulerModule.cpp::CreateTasklet and verifies
            // Tasklet.cpp constructor/destructor lifetime counters exposed by
            // SchedulerModule.cpp C API wrappers and Scheduler.h function slots.
            let scheduler_object = get_scheduler();
            assert!(!scheduler_object.is_null());
            let _scheduler_object = unsafe { PyObject::from_owned_ptr(py, scheduler_object) };

            let all_time_baseline = all_time_tasklets();
            let active_baseline = active_tasklets();
            assert!(all_time_baseline >= 1);
            assert!(active_baseline >= 1);

            let locals = PyDict::new_bound(py);
            py.run_bound(
                r#"
def count_tasklet_callable():
    return None
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let callable = locals.get_item("count_tasklet_callable").unwrap().unwrap();

            let args = PyTuple::new_bound(py, [callable.clone()]);
            let tasklet1 = new_tasklet(api.py_tasklet_type, args.as_ptr());
            assert!(!tasklet1.is_null());
            let tasklet1 = unsafe { PyObject::from_owned_ptr(py, tasklet1) };
            assert_eq!(tasklet_check(tasklet1.as_ptr()), 1);
            assert_eq!(all_time_tasklets(), all_time_baseline + 1);
            assert!(active_tasklets() >= 1);

            let args = PyTuple::new_bound(py, [callable]);
            let tasklet2 = new_tasklet(api.py_tasklet_type, args.as_ptr());
            assert!(!tasklet2.is_null());
            let tasklet2 = unsafe { PyObject::from_owned_ptr(py, tasklet2) };
            assert_eq!(tasklet_check(tasklet2.as_ptr()), 1);
            assert_eq!(all_time_tasklets(), all_time_baseline + 2);
            let active_with_both_tasklets = active_tasklets();
            assert!(active_with_both_tasklets >= 2);

            drop(tasklet1);
            assert_eq!(all_time_tasklets(), all_time_baseline + 2);
            let active_after_first_drop = active_tasklets();
            assert!(active_after_first_drop <= active_with_both_tasklets - 1);

            drop(tasklet2);
            assert_eq!(all_time_tasklets(), all_time_baseline + 2);
            assert!(active_tasklets() <= active_after_first_drop - 1);
        });
    }

    #[test]
    fn c_api_scheduler_schedule_timeout_and_callbacks_match_legacy_capi_smoke_paths() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let schedule: CApiPyObjectObjectInt =
                c_api_fn(api.py_scheduler_schedule, "PyScheduler_Schedule");
            let run_with_timeout: CApiPyObjectI64 = c_api_fn(
                api.py_scheduler_run_with_timeout,
                "PyScheduler_RunWithTimeout",
            );
            let set_channel_callback: CApiIntObject = c_api_fn(
                api.py_scheduler_set_channel_callback,
                "PyScheduler_SetChannelCallback",
            );
            let get_channel_callback: CApiPyObject = c_api_fn(
                api.py_scheduler_get_channel_callback,
                "PyScheduler_GetChannelCallback",
            );
            let set_schedule_callback: CApiIntObject = c_api_fn(
                api.py_scheduler_set_schedule_callback,
                "PyScheduler_SetScheduleCallback",
            );
            let set_fast_callback: CApiVoidScheduleHook = c_api_fn(
                api.py_scheduler_set_schedule_fast_callback,
                "PyScheduler_SetScheduleFastCallback",
            );
            let completed: CApiInt = c_api_fn(
                api.py_scheduler_get_tasklets_completed_last_run_with_timeout,
                "PyScheduler_GetTaskletsCompletedLastRunWithTimeout",
            );
            let switched: CApiInt = c_api_fn(
                api.py_scheduler_get_tasklets_switched_last_run_with_timeout,
                "PyScheduler_GetTaskletsSwitchedLastRunWithTimeout",
            );

            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &module).unwrap();
            py.run_bound(
                r#"
# Parity source: capiTest/Scheduler.cpp::{PyScheduler_Schedule,
# PyScheduler_RunForTime,PyScheduler_SetChannelCallback,
# PyScheduler_GetChannelCallback,PyScheduler_SetScheduleCallback,
# PyScheduler_SetScheduleFastcallback,
# PyScheduler_GetTaskletsCompletedLastRunWithTimeout,
# PyScheduler_GetTaskletsSwitchedLastRunWithTimeout}.
schedule_values = []
scheduler.tasklet(lambda: schedule_values.append("scheduled"))()
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let scheduled = schedule(ptr::null_mut(), 0);
            assert!(!scheduled.is_null());
            unsafe {
                ffi::Py_DECREF(scheduled);
            }
            py.run_bound(
                r#"
assert schedule_values == ["scheduled"]
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();

            py.run_bound(
                r#"
timeout_values = []
scheduler.tasklet(lambda: timeout_values.append(1))()
scheduler.tasklet(lambda: timeout_values.append(2))()
scheduler.tasklet(lambda: timeout_values.append(3))()
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let timeout_result = run_with_timeout(1_000_000_000);
            assert!(!timeout_result.is_null());
            unsafe {
                ffi::Py_DECREF(timeout_result);
            }
            py.run_bound(
                r#"
assert timeout_values == [1, 2, 3]
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(completed(), 3);
            assert_eq!(switched(), 6);

            py.run_bound(
                r#"
limited_timeout_values = []
scheduler.tasklet(lambda: limited_timeout_values.append(1))()
scheduler.tasklet(lambda: limited_timeout_values.append(2))()
scheduler.tasklet(lambda: limited_timeout_values.append(3))()
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let timeout_result = run_with_timeout(0);
            assert!(!timeout_result.is_null());
            unsafe {
                ffi::Py_DECREF(timeout_result);
            }
            py.run_bound(
                r#"
assert limited_timeout_values == [1]
assert scheduler.getruncount() == 3
scheduler.run()
assert limited_timeout_values == [1, 2, 3]
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(completed(), 1);
            assert_eq!(switched(), 2);

            py.run_bound(
                r#"
channel_events = []
def channel_callback(channel, tasklet, sending, will_block):
    channel_events.append((channel, tasklet, sending, will_block))

channel = scheduler.channel()
channel_tasklet = scheduler.tasklet(lambda: channel.send(5))()
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let channel_callback = locals.get_item("channel_callback").unwrap().unwrap();
            assert_eq!(set_channel_callback(channel_callback.as_ptr()), 0);
            let callback_ptr = get_channel_callback();
            assert!(!callback_ptr.is_null());
            let callback_object = unsafe { PyObject::from_owned_ptr(py, callback_ptr) };
            assert!(callback_object.bind(py).is(&channel_callback));
            module
                .getattr("run_n_tasklets")
                .unwrap()
                .call1((1,))
                .unwrap();
            py.run_bound(
                r#"
assert len(channel_events) == 1
assert channel_events[0] == (channel, channel_tasklet, True, True)
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            assert_eq!(set_channel_callback(ptr::null_mut()), 0);

            py.run_bound(
                r#"
schedule_events = []
def schedule_callback(previous, next):
    schedule_events.append((previous, next))

schedule_tasklet = scheduler.tasklet(lambda: None)()
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let schedule_callback = locals.get_item("schedule_callback").unwrap().unwrap();
            assert_eq!(set_schedule_callback(schedule_callback.as_ptr()), 0);
            let schedule_callback_getter = module.getattr("get_schedule_callback").unwrap();
            assert!(schedule_callback_getter
                .call0()
                .unwrap()
                .is(&schedule_callback));
            FAST_FROM.store(ptr::null_mut(), std::sync::atomic::Ordering::SeqCst);
            FAST_TO.store(ptr::null_mut(), std::sync::atomic::Ordering::SeqCst);
            set_fast_callback(Some(record_fast_schedule_callback));
            module
                .getattr("run_n_tasklets")
                .unwrap()
                .call1((1,))
                .unwrap();
            py.run_bound(
                r#"
assert schedule_events[-1] == (schedule_tasklet, scheduler.getcurrent())
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let schedule_tasklet = locals.get_item("schedule_tasklet").unwrap().unwrap();
            let current = module.getattr("getcurrent").unwrap().call0().unwrap();
            assert_eq!(
                FAST_FROM.load(std::sync::atomic::Ordering::SeqCst),
                schedule_tasklet.as_ptr()
            );
            assert_eq!(
                FAST_TO.load(std::sync::atomic::Ordering::SeqCst),
                current.as_ptr()
            );
            assert_eq!(set_schedule_callback(ptr::null_mut()), 0);
            assert!(schedule_callback_getter.call0().unwrap().is_none());
            set_fast_callback(None);
        });
    }

    #[test]
    fn active_channel_count_matches_legacy_teardown_and_capi_refcounts() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_utils.py::SchedulerTestCaseBase.tearDown checks
# get_number_of_active_channels() after unblock_all_channels()+gc.
import gc

for _ in range(3):
    gc.collect()
baseline = scheduler.get_number_of_active_channels()
channel = scheduler.channel()
after_create = scheduler.get_number_of_active_channels()
assert after_create == baseline + 1
del channel
for _ in range(3):
    gc.collect()
assert scheduler.get_number_of_active_channels() <= baseline
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("active channel count should track Python channel lifetime");

            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let active_channels: CApiInt = c_api_fn(
                api.py_scheduler_get_number_of_active_channels,
                "PyScheduler_GetNumberOfActiveChannels",
            );
            let new_channel: CApiPyObjectPyType = c_api_fn(api.py_channel_new, "PyChannel_New");

            // Parity source:
            // capiTest/Scheduler.cpp::PyScheduler_GetNumberOfActiveChannels
            // expects +1 per PyChannel_New and -1 per Py_DECREF.
            let baseline = active_channels();
            let channel_type = py.get_type_bound::<Channel>().as_type_ptr();
            let channel1 = new_channel(channel_type);
            assert!(!channel1.is_null());
            let after_channel1 = active_channels();
            assert_eq!(after_channel1, baseline + 1);
            let channel2 = new_channel(channel_type);
            assert!(!channel2.is_null());
            let after_channel2 = active_channels();
            assert_eq!(after_channel2, after_channel1 + 1);
            unsafe {
                ffi::Py_DECREF(channel1);
            }
            let after_drop1 = active_channels();
            assert_eq!(after_drop1, after_channel2 - 1);
            unsafe {
                ffi::Py_DECREF(channel2);
            }
            assert_eq!(active_channels(), after_drop1 - 1);
        });
    }

    #[test]
    fn unblock_all_channels_matches_legacy_teardown_cleanup() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_utils.py::SchedulerTestCaseBase.tearDown calls
# scheduler.run(), scheduler.unblock_all_channels(), gc.collect(), then
# asserts get_number_of_active_channels() == 0. Channel.cpp::
# UnblockAllActiveChannels only counts channels with nonzero balance and
# delegates to Channel::ClearBlocked(false).
import gc
import weakref

scheduler.run()
scheduler.unblock_all_channels()
for _ in range(3):
    gc.collect()

baseline_channels = scheduler.get_number_of_active_channels()
sender_channel = scheduler.channel()
receiver_channel = scheduler.channel()
idle_channel = scheduler.channel()
sender_ref = weakref.ref(sender_channel)
receiver_ref = weakref.ref(receiver_channel)
idle_ref = weakref.ref(idle_channel)

def sender(chan):
    chan.send("payload")

def receiver(chan):
    chan.receive()

sender_tasklet = scheduler.tasklet(sender)(sender_channel)
receiver_tasklet = scheduler.tasklet(receiver)(receiver_channel)
scheduler.run()

assert sender_channel.balance == 1
assert receiver_channel.balance == -1
assert idle_channel.balance == 0
assert sender_tasklet.blocked is True
assert receiver_tasklet.blocked is True

unblocked = scheduler.unblock_all_channels()
assert unblocked >= 2, unblocked
assert sender_channel.balance == 0, sender_channel.balance
assert receiver_channel.balance == 0, receiver_channel.balance
assert idle_channel.balance == 0, idle_channel.balance
assert sender_tasklet.blocked is False, (sender_tasklet.blocked, sender_tasklet.alive)
assert receiver_tasklet.blocked is False, (receiver_tasklet.blocked, receiver_tasklet.alive)
assert sender_tasklet.alive is False, (sender_tasklet.blocked, sender_tasklet.alive)
assert receiver_tasklet.alive is False, (receiver_tasklet.blocked, receiver_tasklet.alive)
scheduler.unblock_all_channels()

scheduler.run()
del sender_tasklet, receiver_tasklet
del sender_channel, receiver_channel, idle_channel
for _ in range(3):
    gc.collect()
active_channels = scheduler.get_number_of_active_channels()
assert sender_ref() is None, ("sender", sender_ref())
assert receiver_ref() is None, ("receiver", receiver_ref())
assert idle_ref() is None, ("idle", idle_ref())
assert active_channels <= baseline_channels, (active_channels, baseline_channels)
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("unblock_all_channels should clear blocked tasklets for teardown");
        });
    }

    #[test]
    fn tasklet_resource_counters_match_legacy_capi_lifetime() {
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "_scheduler").expect("create module");
            populate_scheduler_module(py, &module).expect("populate module");
            let (_capsule, api) = scheduler_c_api_for_test(py);
            let all_time: CApiInt = c_api_fn(
                api.py_scheduler_get_all_time_tasklet_count,
                "PyScheduler_GetAllTimeTaskletCount",
            );
            let active: CApiInt = c_api_fn(
                api.py_scheduler_get_active_tasklet_count,
                "PyScheduler_GetActiveTaskletCount",
            );
            let new_tasklet: CApiPyObjectPyTypeObject =
                c_api_fn(api.py_tasklet_new, "PyTasklet_New");

            // Parity source: capiTest/Scheduler.cpp::
            // {PyScheduler_GetAllTimeTaskletCount,
            // PyScheduler_GetActiveTaskletCount}; Tasklet.cpp increments
            // all-time and active counters in Tasklet::Tasklet and decrements
            // only the active counter in Tasklet::~Tasklet.
            let all_time_baseline = all_time();
            let active_baseline = active();
            assert!(all_time_baseline >= active_baseline);

            let tasklet_type = py.get_type_bound::<Tasklet>().as_type_ptr();
            let tasklet1 = new_tasklet(tasklet_type, ptr::null_mut());
            assert!(!tasklet1.is_null());
            assert_eq!(all_time(), all_time_baseline + 1);
            assert_eq!(active(), active_baseline + 1);

            let tasklet2 = new_tasklet(tasklet_type, ptr::null_mut());
            assert!(!tasklet2.is_null());
            assert_eq!(all_time(), all_time_baseline + 2);
            assert_eq!(active(), active_baseline + 2);

            unsafe {
                ffi::Py_DECREF(tasklet1);
            }
            assert_eq!(all_time(), all_time_baseline + 2);
            assert_eq!(active(), active_baseline + 1);

            unsafe {
                ffi::Py_DECREF(tasklet2);
            }
            assert_eq!(all_time(), all_time_baseline + 2);
            assert_eq!(active(), active_baseline);
        });
    }

    #[test]
    fn schedule_manager_wrapper_lifecycle_matches_legacy_thread_cache() {
        let count_before_thread = get_number_of_active_schedule_managers();

        std::thread::spawn(move || {
            Python::with_gil(|py| {
                let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
                let locals = PyDict::new_bound(py);
                locals.set_item("scheduler", &scheduler).unwrap();

                let (_capsule, api) = scheduler_c_api_for_test(py);
                let get_scheduler: CApiPyObject =
                    c_api_fn(api.py_scheduler_get_scheduler, "PyScheduler_GetScheduler");
                let active_managers: CApiInt = c_api_fn(
                    api.py_scheduler_get_number_of_active_schedule_managers,
                    "PyScheduler_GetNumberOfActiveScheduleManagers",
                );

                // Parity source:
                // ScheduleManager.cpp::GetThreadScheduleManager creates one
                // scheduler-owned manager per Python thread and increments
                // s_numberOfActiveScheduleManagers when it stores the manager
                // in the thread dict. SchedulerModule.cpp::PyScheduler_GetScheduler
                // returns that same thread manager through the C API.
                let count_before_get = active_managers();
                let capi_manager = get_scheduler();
                assert!(!capi_manager.is_null());
                assert_eq!(active_managers(), count_before_get + 1);
                let python_manager = schedule_manager(py).expect("get Python schedule manager");
                assert_eq!(capi_manager, python_manager.as_ptr());
                drop(python_manager);
                unsafe {
                    ffi::Py_DECREF(capi_manager);
                }

                py.run_bound(
                    r#"
# Parity sources:
# - PyScheduleManager.cpp/PyScheduleManager.h expose a weakref list on
#   schedule_manager objects and clear it during dealloc.
# - SchedulerModule.cpp::SchedulerGetScheduleManager returns a new reference
#   to the cached thread manager.
# - test_utils.py::SchedulerTestCaseBase.tearDown asserts
#   sys.getrefcount(scheduler.get_schedule_manager()) == 2 and one active
#   schedule manager after GC.
import gc
import sys
import weakref

assert scheduler.get_number_of_active_schedule_managers() >= 1
manager = scheduler.get_schedule_manager()
assert type(manager) is scheduler.schedule_manager
assert isinstance(manager, scheduler.schedule_manager)
assert manager is scheduler.get_schedule_manager()

manager_ref = weakref.ref(manager)
assert manager_ref() is manager

direct = scheduler.schedule_manager()
assert type(direct) is scheduler.schedule_manager
direct_ref = weakref.ref(direct)
del direct
for _ in range(3):
    gc.collect()
assert direct_ref() is None

del manager
for _ in range(3):
    gc.collect()
assert manager_ref() is scheduler.get_schedule_manager()
assert sys.getrefcount(scheduler.get_schedule_manager()) == 2
"#,
                    Some(&locals),
                    Some(&locals),
                )
                .expect("schedule manager wrapper lifecycle should match legacy sources");
            });
        })
        .join()
        .expect("schedule manager parity thread should finish");

        assert_eq!(
            get_number_of_active_schedule_managers(),
            count_before_thread,
            "thread-local schedule manager count should return to its pre-thread value after teardown",
        );
    }

    #[test]
    fn switch_trap_blocks_channel_operations_before_callbacks_like_legacy_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_scheduler.py::TestSwitchTrap.{test_send,
# test_receive,test_send_exception,test_send_throw}. Switch-trapped channel
# operations raise RuntimeError("switch_trap") before a channel callback runs
# or the scheduler drains queued tasklets.
import gc

while scheduler.switch_trap(0) > 0:
    scheduler.switch_trap(-1)

events = []
def channel_callback(channel, tasklet, sending, will_block):
    events.append((channel, tasklet, sending, will_block))

channel = scheduler.channel()
scheduler.set_channel_callback(channel_callback)
try:
    scheduler.switch_trap(1)
    operations = [
        ("send", lambda: channel.send(None)),
        ("receive", lambda: channel.receive()),
        ("send_exception", lambda: channel.send_exception(RuntimeError)),
        ("send_throw", lambda: channel.send_throw(RuntimeError)),
    ]
    for name, operation in operations:
        try:
            operation()
        except RuntimeError as exc:
            assert str(exc) == "switch_trap", (name, exc)
        else:
            raise AssertionError(f"{name} did not raise switch_trap")
    assert events == []
finally:
    while scheduler.switch_trap(0) > 0:
        scheduler.switch_trap(-1)
    scheduler.set_channel_callback(None)
    del channel
    gc.collect()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("switch trap should guard channel operations");
        });
    }

    #[test]
    fn python_schedule_and_switch_trap_entrypoints_match_test_scheduler_paths() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_scheduler.py::TestSchedule.test_schedule and
# test_scheduler.py::TestSwitchTrap.{test_schedule,test_schedule_remove,test_run,
# test_run_specific,test_run_paused,test_run_raising_function}.
while scheduler.switch_trap(0) > 0:
    scheduler.switch_trap(-1)

def assert_switch_trap(callable_):
    try:
        callable_()
    except RuntimeError as exc:
        assert str(exc) == "switch_trap", exc
    else:
        raise AssertionError("expected RuntimeError('switch_trap')")

schedule_events = []
def scheduled_foo(previous):
    schedule_events.append(("foo", previous.scheduled))

scheduled = scheduler.tasklet(scheduled_foo)(scheduler.getcurrent())
assert scheduler.getruncount() == 2
assert scheduled.scheduled is True
scheduler.schedule()
assert schedule_events == [("foo", True)]
assert scheduler.getruncount() == 1

run_events = []
queued_for_schedule = scheduler.tasklet(lambda: run_events.append("schedule"))()
scheduler.switch_trap(1)
try:
    assert_switch_trap(scheduler.schedule)
finally:
    scheduler.switch_trap(-1)
assert queued_for_schedule.scheduled is True
scheduler.run()
assert run_events == ["schedule"]
assert scheduler.getruncount() == 1

schedule_remove_main = []
queued_for_schedule_remove = scheduler.tasklet(lambda: schedule_remove_main[0].insert())()
scheduler.switch_trap(1)
try:
    assert_switch_trap(scheduler.schedule_remove)
finally:
    scheduler.switch_trap(-1)
assert queued_for_schedule_remove.scheduled is True
schedule_remove_main.append(scheduler.getcurrent())
scheduler.schedule_remove()
assert scheduler.getruncount() == 1

queued_for_run = scheduler.tasklet(lambda: run_events.append("run"))()
scheduler.switch_trap(1)
try:
    assert_switch_trap(scheduler.run)
finally:
    scheduler.switch_trap(-1)
assert queued_for_run.scheduled is True
scheduler.run()
assert run_events[-1] == "run"
assert scheduler.getruncount() == 1

specific = scheduler.tasklet(lambda: run_events.append("specific"))()
scheduler.switch_trap(1)
try:
    assert_switch_trap(specific.run)
finally:
    scheduler.switch_trap(-1)
assert specific.scheduled is True
specific.run()
assert run_events[-1] == "specific"
assert scheduler.getruncount() == 1

paused = scheduler.tasklet(lambda: run_events.append("paused"))
paused.bind(args=())
assert paused.paused is True
scheduler.switch_trap(1)
try:
    assert_switch_trap(paused.run)
finally:
    scheduler.switch_trap(-1)
assert paused.paused is True
paused.run()
assert run_events[-1] == "paused"
assert scheduler.getruncount() == 1

def boom():
    raise RuntimeError("boom!")

scheduler.tasklet(boom)()
try:
    scheduler.run()
except RuntimeError as exc:
    assert str(exc) == "boom!"
else:
    raise AssertionError("scheduler.run should propagate tasklet exceptions")
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("schedule and switch_trap scheduler entry points should match legacy tests");
        });
    }

    #[test]
    fn python_switch_raise_exception_and_kill_match_test_scheduler_paths() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_scheduler.py::TestSwitch.{test_switch,
# test_switch_blocked,test_switch_trapped} and
# test_scheduler.py::TestSwitchTrap.{test_raise_exception,test_kill}.
import gc

while scheduler.switch_trap(0) > 0:
    scheduler.switch_trap(-1)
scheduler.set_channel_callback(None)
scheduler.set_schedule_callback(None)
for _ in range(2):
    try:
        scheduler.run()
    except Exception:
        pass
    scheduler.unblock_all_channels()
gc.collect()

def assert_runtime_message(callable_, message):
    try:
        callable_()
    except RuntimeError as exc:
        assert message in str(exc), exc
    else:
        raise AssertionError(f"expected RuntimeError containing {message!r}")

finished = []
def target():
    finished.append("target")

switch_tasklet = scheduler.tasklet(target)()
assert scheduler.getruncount() == 2
switch_tasklet.switch()
assert finished == ["target"]
assert scheduler.getruncount() == 1

trapped_tasklet = scheduler.tasklet(lambda: finished.append("trapped"))()
scheduler.switch_trap(1)
try:
    assert_runtime_message(trapped_tasklet.switch, "switch_trap")
finally:
    scheduler.switch_trap(-1)
assert trapped_tasklet.scheduled is True
trapped_tasklet.switch()
assert finished == ["target", "trapped"]
assert scheduler.getruncount() == 1

source = scheduler.getcurrent()
paused_events = []
def paused_target():
    assert source.paused is True
    source.insert()
    paused_events.append("paused")

paused_tasklet = scheduler.tasklet(paused_target)
paused_tasklet.bind(args=())
assert paused_tasklet.paused is True
scheduler.switch_trap(1)
try:
    assert_runtime_message(paused_tasklet.switch, "switch_trap")
finally:
    scheduler.switch_trap(-1)
assert paused_tasklet.paused is True
paused_tasklet.switch()
assert paused_events == ["paused"]
assert paused_tasklet.alive is False
assert source.paused is False
assert scheduler.getruncount() == 1

blocked_finished = []
blocked_channel = scheduler.channel()
def blocked_target():
    blocked_channel.receive()
    blocked_finished.append("done")

blocked = scheduler.tasklet(blocked_target)()
blocked.run()
assert scheduler.getruncount() == 1
assert blocked.blocked is True
assert_runtime_message(blocked.switch, "blocked")
blocked_channel.send(None)
assert blocked.blocked is False
assert blocked_finished == ["done"]
assert blocked.blocked is False
assert blocked.alive is False
assert blocked_channel.balance == 0
assert scheduler.getruncount() == 1

raise_channel = scheduler.channel()
raised_events = []
def receiver_for_raise_exception():
    try:
        raise_channel.receive()
    except IndexError:
        raised_events.append("caught")

raised = scheduler.tasklet(receiver_for_raise_exception)()
raised.run()
assert raised.blocked is True
scheduler.switch_trap(1)
try:
    assert_runtime_message(lambda: raised.raise_exception(RuntimeError), "switch_trap")
finally:
    scheduler.switch_trap(-1)
assert raised.blocked is True
raised.raise_exception(IndexError)
assert raised_events == ["caught"], raised_events
assert raised.alive is False
assert raise_channel.balance == 0
assert scheduler.getruncount() == 1

kill_channel = scheduler.channel()
def receiver_for_kill():
    kill_channel.receive()

killed = scheduler.tasklet(receiver_for_kill)()
killed.run()
assert killed.blocked is True
scheduler.switch_trap(1)
try:
    assert_runtime_message(killed.kill, "switch_trap")
finally:
    scheduler.switch_trap(-1)
assert killed.blocked is True
killed.kill()
assert killed.alive is False, (killed.alive, killed.blocked, kill_channel.balance)
kill_channel.clear()

del blocked_channel, raise_channel, kill_channel
gc.collect()
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("switch, raise_exception, and kill paths should match legacy scheduler tests");
        });
    }

    #[test]
    fn python_schedule_and_channel_callbacks_match_legacy_callback_tests() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_scheduler.py::TestSchedule.test_schedule_callback_basic.
schedule_output = []
def schedule_callback(previous_tasklet, next_tasklet):
    schedule_output.append(previous_tasklet)
    schedule_output.append(next_tasklet)

try:
    scheduler.set_schedule_callback(schedule_callback)
    main = scheduler.getmain()
    t1 = scheduler.tasklet(lambda: None)()
    t2 = scheduler.tasklet(lambda: None)()
    t3 = scheduler.tasklet(lambda: None)()
    scheduler.run()
    assert scheduler.getruncount() == 1
    assert schedule_output == [
        main, t1,
        t1, main,
        main, t2,
        t2, main,
        main, t3,
        t3, main,
    ]
finally:
    scheduler.set_schedule_callback(None)

# Parity source:
# test_scheduler.py::TestSchedule.test_schedule_callback_with_multiple_threads.
import threading

callback_calls = [0]
callback_threads = []
thread_errors = []
def threaded_schedule_callback(previous_tasklet, next_tasklet):
    callback_calls[0] += 1
    callback_threads.append(threading.get_ident())

def create_threaded_tasklets():
    try:
        for _ in range(2):
            scheduler.tasklet(lambda: None)()
        scheduler.run()
    except BaseException as exc:
        thread_errors.append(exc)

try:
    scheduler.set_schedule_callback(threaded_schedule_callback)
    thread = threading.Thread(target=create_threaded_tasklets)
    thread.start()
    create_threaded_tasklets()
    thread.join()
    assert thread_errors == [], thread_errors
    assert callback_calls[0] >= 8 and callback_calls[0] % 2 == 0, (callback_calls, callback_threads)
finally:
    scheduler.set_schedule_callback(None)

# Parity source: test_channel.py::TestChannels.
# {test_channel_callback_with_blocking_send,
# test_channel_callback_with_blocking_receive}.
def run_channel_callback_case(receive_first):
    callback_output = []
    def channel_callback(channel, tasklet, is_sending, will_block):
        callback_output.append([channel, tasklet, is_sending, will_block])

    scheduler.set_channel_callback(channel_callback)
    try:
        channel = scheduler.channel()
        value = "VALUE"

        def sending_tasklet():
            channel.send(value)

        def receiving_tasklet():
            received = channel.receive()
            assert received == value

        if receive_first:
            first = scheduler.tasklet(receiving_tasklet)()
            second = scheduler.tasklet(sending_tasklet)()
            expected = [
                [channel, first, False, True],
                [channel, second, True, False],
            ]
        else:
            first = scheduler.tasklet(sending_tasklet)()
            second = scheduler.tasklet(receiving_tasklet)()
            expected = [
                [channel, first, True, True],
                [channel, second, False, False],
            ]

        scheduler.run()
        assert callback_output == expected, (callback_output, expected)
        assert channel.balance == 0
    finally:
        scheduler.set_channel_callback(None)

run_channel_callback_case(False)
run_channel_callback_case(True)
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("Python callback ordering and will_block flags should match legacy tests");
        });
    }

    #[test]
    fn python_non_main_run_matches_legacy_test_run_path() {
        Python::with_gil(|py| {
            let scheduler = load_legacy_scheduler_module(py).expect("load scheduler package");
            let locals = PyDict::new_bound(py);
            locals.set_item("scheduler", &scheduler).unwrap();

            py.run_bound(
                r#"
# Parity source: test_scheduler.py::TestRun.test_calling_run_from_non_main_tasklet.
scheduler.set_schedule_callback(None)
scheduler.set_channel_callback(None)
values = []
def foo(value):
    values.append(value)

def bar(chan):
    scheduler.tasklet(foo)("a")
    scheduler.tasklet(foo)("b")
    scheduler.tasklet(foo)("c")
    scheduler.tasklet(foo)("d")
    chan.send(1)
    scheduler.tasklet(foo)("e")
    scheduler.tasklet(foo)("f")
    scheduler.tasklet(foo)("g")
    scheduler.run()

channel = scheduler.channel()
t = scheduler.tasklet(bar)(channel)
t.run()
channel.receive()
scheduler.run()
assert values == ["a", "b", "c", "d", "e", "f", "g"], values
assert scheduler.getruncount() == 1
"#,
                Some(&locals),
                Some(&locals),
            )
            .expect("scheduler.run from a non-main tasklet should match legacy TestRun behavior");
        });
    }

    type CApiInt = extern "C" fn() -> i32;
    type CApiIntObject = extern "C" fn(*mut ffi::PyObject) -> i32;
    type CApiIntObjectObject = extern "C" fn(*mut ffi::PyObject, *mut ffi::PyObject) -> i32;
    type CApiIntObjectObjectObject =
        extern "C" fn(*mut ffi::PyObject, *mut ffi::PyObject, *mut ffi::PyObject) -> i32;
    type CApiIntObjectObjectObjectObject = extern "C" fn(
        *mut ffi::PyObject,
        *mut ffi::PyObject,
        *mut ffi::PyObject,
        *mut ffi::PyObject,
    ) -> i32;
    type CApiLongObject = extern "C" fn(*mut ffi::PyObject) -> c_long;
    type CApiCStringObject = extern "C" fn(*mut ffi::PyObject) -> *const c_char;
    type CApiPyObject = extern "C" fn() -> *mut ffi::PyObject;
    type CApiPyObjectObject = extern "C" fn(*mut ffi::PyObject) -> *mut ffi::PyObject;
    type CApiPyObjectObjectInt = extern "C" fn(*mut ffi::PyObject, i32) -> *mut ffi::PyObject;
    type CApiPyObjectInt = extern "C" fn(i32) -> *mut ffi::PyObject;
    type CApiPyObjectI64 = extern "C" fn(i64) -> *mut ffi::PyObject;
    type CApiPyObjectPyTypeObject =
        extern "C" fn(*mut ffi::PyTypeObject, *mut ffi::PyObject) -> *mut ffi::PyObject;
    type CApiPyObjectPyType = extern "C" fn(*mut ffi::PyTypeObject) -> *mut ffi::PyObject;
    type CApiVoidObjectInt = extern "C" fn(*mut ffi::PyObject, i32);
    type CApiVoidScheduleHook = extern "C" fn(Option<ScheduleHookFunc>);

    static FAST_FROM: std::sync::atomic::AtomicPtr<ffi::PyObject> =
        std::sync::atomic::AtomicPtr::new(ptr::null_mut());
    static FAST_TO: std::sync::atomic::AtomicPtr<ffi::PyObject> =
        std::sync::atomic::AtomicPtr::new(ptr::null_mut());

    extern "C" fn record_fast_schedule_callback(
        from: *mut ffi::PyObject,
        to: *mut ffi::PyObject,
    ) -> i32 {
        FAST_FROM.store(from, std::sync::atomic::Ordering::SeqCst);
        FAST_TO.store(to, std::sync::atomic::Ordering::SeqCst);
        0
    }

    fn scheduler_c_api_for_test(py: Python<'_>) -> (PyObject, &'static SchedulerCapsuleApi) {
        let capsule = create_scheduler_c_api_capsule(py).expect("create C API capsule");
        let api = unsafe {
            ffi::PyCapsule_GetPointer(capsule.as_ptr(), scheduler_capsule_name_ptr())
                .cast::<SchedulerCapsuleApi>()
        };
        assert!(!api.is_null());
        (capsule, unsafe { &*api })
    }

    fn scheduler_c_api_import_for_test(py: Python<'_>) -> &'static SchedulerCapsuleApi {
        let api = unsafe {
            ffi::PyCapsule_Import(scheduler_capsule_name_ptr(), 0).cast::<SchedulerCapsuleApi>()
        };
        if api.is_null() {
            panic!(
                "PyCapsule_Import({SCHEDULER_CAPSULE_NAME}) failed: {:?}",
                PyErr::fetch(py)
            );
        }
        unsafe { &*api }
    }

    fn c_api_fn<T>(pointer: *mut c_void, name: &str) -> T
    where
        T: Copy,
    {
        assert!(!pointer.is_null(), "{name} function pointer is null");
        unsafe { std::mem::transmute_copy(&pointer) }
    }

    fn scheduler_c_api_offsets() -> [usize; carbon_scheduler_ffi::SCHEDULER_C_API_FIELD_COUNT] {
        [
            scheduler_api_offset_of!(py_tasklet_new),
            scheduler_api_offset_of!(py_tasklet_setup),
            scheduler_api_offset_of!(py_tasklet_insert),
            scheduler_api_offset_of!(py_tasklet_get_block_trap),
            scheduler_api_offset_of!(py_tasklet_set_block_trap),
            scheduler_api_offset_of!(py_tasklet_is_main),
            scheduler_api_offset_of!(py_tasklet_check),
            scheduler_api_offset_of!(py_tasklet_alive),
            scheduler_api_offset_of!(py_tasklet_kill),
            scheduler_api_offset_of!(py_channel_new),
            scheduler_api_offset_of!(py_channel_send),
            scheduler_api_offset_of!(py_channel_receive),
            scheduler_api_offset_of!(py_channel_send_exception),
            scheduler_api_offset_of!(py_channel_get_queue),
            scheduler_api_offset_of!(py_channel_get_preference),
            scheduler_api_offset_of!(py_channel_set_preference),
            scheduler_api_offset_of!(py_channel_get_balance),
            scheduler_api_offset_of!(py_channel_check),
            scheduler_api_offset_of!(py_channel_send_throw),
            scheduler_api_offset_of!(py_scheduler_get_scheduler),
            scheduler_api_offset_of!(py_scheduler_schedule),
            scheduler_api_offset_of!(py_scheduler_get_run_count),
            scheduler_api_offset_of!(py_scheduler_get_current),
            scheduler_api_offset_of!(py_scheduler_run_with_timeout),
            scheduler_api_offset_of!(py_scheduler_run_n_tasklets),
            scheduler_api_offset_of!(py_scheduler_set_channel_callback),
            scheduler_api_offset_of!(py_scheduler_get_channel_callback),
            scheduler_api_offset_of!(py_scheduler_set_schedule_callback),
            scheduler_api_offset_of!(py_scheduler_set_schedule_fast_callback),
            scheduler_api_offset_of!(py_scheduler_get_number_of_active_schedule_managers),
            scheduler_api_offset_of!(py_scheduler_get_number_of_active_channels),
            scheduler_api_offset_of!(py_scheduler_get_all_time_tasklet_count),
            scheduler_api_offset_of!(py_scheduler_get_active_tasklet_count),
            scheduler_api_offset_of!(py_scheduler_get_tasklets_completed_last_run_with_timeout),
            scheduler_api_offset_of!(py_scheduler_get_tasklets_switched_last_run_with_timeout),
            scheduler_api_offset_of!(py_tasklet_type),
            scheduler_api_offset_of!(py_channel_type),
            scheduler_api_offset_of!(tasklet_exit),
            scheduler_api_offset_of!(py_tasklet_get_times_switched_to),
            scheduler_api_offset_of!(py_tasklet_get_context),
        ]
    }

    fn scheduler_c_api_entries(
        api: &SchedulerCapsuleApi,
    ) -> [*mut c_void; carbon_scheduler_ffi::SCHEDULER_C_API_FIELD_COUNT] {
        [
            api.py_tasklet_new,
            api.py_tasklet_setup,
            api.py_tasklet_insert,
            api.py_tasklet_get_block_trap,
            api.py_tasklet_set_block_trap,
            api.py_tasklet_is_main,
            api.py_tasklet_check,
            api.py_tasklet_alive,
            api.py_tasklet_kill,
            api.py_channel_new,
            api.py_channel_send,
            api.py_channel_receive,
            api.py_channel_send_exception,
            api.py_channel_get_queue,
            api.py_channel_get_preference,
            api.py_channel_set_preference,
            api.py_channel_get_balance,
            api.py_channel_check,
            api.py_channel_send_throw,
            api.py_scheduler_get_scheduler,
            api.py_scheduler_schedule,
            api.py_scheduler_get_run_count,
            api.py_scheduler_get_current,
            api.py_scheduler_run_with_timeout,
            api.py_scheduler_run_n_tasklets,
            api.py_scheduler_set_channel_callback,
            api.py_scheduler_get_channel_callback,
            api.py_scheduler_set_schedule_callback,
            api.py_scheduler_set_schedule_fast_callback,
            api.py_scheduler_get_number_of_active_schedule_managers,
            api.py_scheduler_get_number_of_active_channels,
            api.py_scheduler_get_all_time_tasklet_count,
            api.py_scheduler_get_active_tasklet_count,
            api.py_scheduler_get_tasklets_completed_last_run_with_timeout,
            api.py_scheduler_get_tasklets_switched_last_run_with_timeout,
            api.py_tasklet_type.cast::<c_void>(),
            api.py_channel_type.cast::<c_void>(),
            api.tasklet_exit.cast::<c_void>(),
            api.py_tasklet_get_times_switched_to,
            api.py_tasklet_get_context,
        ]
    }

    fn run_count(module: &Bound<'_, PyModule>) -> u32 {
        module
            .getattr("getruncount")
            .unwrap()
            .call0()
            .unwrap()
            .extract()
            .unwrap()
    }

    fn load_legacy_scheduler_module<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyModule>> {
        let rust_module = PyModule::new_bound(py, "_scheduler")?;
        populate_scheduler_module(py, &rust_module)?;

        let sys = py.import_bound("sys")?;
        let modules = sys.getattr("modules")?;
        modules.set_item("_scheduler", &rust_module)?;

        static LEGACY_MODULE_COUNTER: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let module_name = format!(
            "scheduler_legacy_test_{}",
            LEGACY_MODULE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        );
        let scheduler = PyModule::from_code_bound(
            py,
            LEGACY_SCHEDULER_INIT,
            "carbonengine/scheduler/python/scheduler/__init__.py",
            &module_name,
        )?;
        modules.set_item("scheduler", &scheduler)?;
        modules.set_item(module_name, &scheduler)?;
        Ok(scheduler)
    }

    fn py_len(py: Python<'_>, locals: &Bound<'_, PyDict>, name: &str) -> usize {
        py.eval_bound(&format!("len({name})"), None, Some(locals))
            .expect("evaluate len")
            .extract()
            .expect("extract len")
    }
}
