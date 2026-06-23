use std::ffi::c_void;
use std::panic::{catch_unwind, UnwindSafe};

pub const CARBON_SCHEDULER_ABI_VERSION: u32 = 1;
pub const SCHEDULER_C_API_CAPSULE_NAME: &str = "scheduler._C_API";
pub const SCHEDULER_C_API_CAPSULE_NAME_CSTR: &[u8] = b"scheduler._C_API\0";

pub const SCHEDULER_C_API_FIELD_NAMES: [&str; SCHEDULER_C_API_FIELD_COUNT] = [
    "PyTasklet_New",
    "PyTasklet_Setup",
    "PyTasklet_Insert",
    "PyTasklet_GetBlockTrap",
    "PyTasklet_SetBlockTrap",
    "PyTasklet_IsMain",
    "PyTasklet_Check",
    "PyTasklet_Alive",
    "PyTasklet_Kill",
    "PyChannel_New",
    "PyChannel_Send",
    "PyChannel_Receive",
    "PyChannel_SendException",
    "PyChannel_GetQueue",
    "PyChannel_GetPreference",
    "PyChannel_SetPreference",
    "PyChannel_GetBalance",
    "PyChannel_Check",
    "PyChannel_SendThrow",
    "PyScheduler_GetScheduler",
    "PyScheduler_Schedule",
    "PyScheduler_GetRunCount",
    "PyScheduler_GetCurrent",
    "PyScheduler_RunWithTimeout",
    "PyScheduler_RunNTasklets",
    "PyScheduler_SetChannelCallback",
    "PyScheduler_GetChannelCallback",
    "PyScheduler_SetScheduleCallback",
    "PyScheduler_SetScheduleFastCallback",
    "PyScheduler_GetNumberOfActiveScheduleManagers",
    "PyScheduler_GetNumberOfActiveChannels",
    "PyScheduler_GetAllTimeTaskletCount",
    "PyScheduler_GetActiveTaskletCount",
    "PyScheduler_GetTaskletsCompletedLastRunWithTimeout",
    "PyScheduler_GetTaskletsSwitchedLastRunWithTimeout",
    "PyTaskletType",
    "PyChannelType",
    "TaskletExit",
    "PyTasklet_GetTimesSwitchedTo",
    "PyTasklet_GetContext",
];

pub const SCHEDULER_C_API_FIELD_COUNT: usize = 40;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SchedulerCapi {
    pub py_tasklet_new: *mut c_void,
    pub py_tasklet_setup: *mut c_void,
    pub py_tasklet_insert: *mut c_void,
    pub py_tasklet_get_block_trap: *mut c_void,
    pub py_tasklet_set_block_trap: *mut c_void,
    pub py_tasklet_is_main: *mut c_void,
    pub py_tasklet_check: *mut c_void,
    pub py_tasklet_alive: *mut c_void,
    pub py_tasklet_kill: *mut c_void,
    pub py_channel_new: *mut c_void,
    pub py_channel_send: *mut c_void,
    pub py_channel_receive: *mut c_void,
    pub py_channel_send_exception: *mut c_void,
    pub py_channel_get_queue: *mut c_void,
    pub py_channel_get_preference: *mut c_void,
    pub py_channel_set_preference: *mut c_void,
    pub py_channel_get_balance: *mut c_void,
    pub py_channel_check: *mut c_void,
    pub py_channel_send_throw: *mut c_void,
    pub py_scheduler_get_scheduler: *mut c_void,
    pub py_scheduler_schedule: *mut c_void,
    pub py_scheduler_get_run_count: *mut c_void,
    pub py_scheduler_get_current: *mut c_void,
    pub py_scheduler_run_with_timeout: *mut c_void,
    pub py_scheduler_run_n_tasklets: *mut c_void,
    pub py_scheduler_set_channel_callback: *mut c_void,
    pub py_scheduler_get_channel_callback: *mut c_void,
    pub py_scheduler_set_schedule_callback: *mut c_void,
    pub py_scheduler_set_schedule_fast_callback: *mut c_void,
    pub py_scheduler_get_number_of_active_schedule_managers: *mut c_void,
    pub py_scheduler_get_number_of_active_channels: *mut c_void,
    pub py_scheduler_get_all_time_tasklet_count: *mut c_void,
    pub py_scheduler_get_active_tasklet_count: *mut c_void,
    pub py_scheduler_get_tasklets_completed_last_run_with_timeout: *mut c_void,
    pub py_scheduler_get_tasklets_switched_last_run_with_timeout: *mut c_void,
    pub py_tasklet_type: *mut c_void,
    pub py_channel_type: *mut c_void,
    pub tasklet_exit: *mut *mut c_void,
    pub py_tasklet_get_times_switched_to: *mut c_void,
    pub py_tasklet_get_context: *mut c_void,
}

impl SchedulerCapi {
    pub const fn empty() -> Self {
        Self {
            py_tasklet_new: std::ptr::null_mut(),
            py_tasklet_setup: std::ptr::null_mut(),
            py_tasklet_insert: std::ptr::null_mut(),
            py_tasklet_get_block_trap: std::ptr::null_mut(),
            py_tasklet_set_block_trap: std::ptr::null_mut(),
            py_tasklet_is_main: std::ptr::null_mut(),
            py_tasklet_check: std::ptr::null_mut(),
            py_tasklet_alive: std::ptr::null_mut(),
            py_tasklet_kill: std::ptr::null_mut(),
            py_channel_new: std::ptr::null_mut(),
            py_channel_send: std::ptr::null_mut(),
            py_channel_receive: std::ptr::null_mut(),
            py_channel_send_exception: std::ptr::null_mut(),
            py_channel_get_queue: std::ptr::null_mut(),
            py_channel_get_preference: std::ptr::null_mut(),
            py_channel_set_preference: std::ptr::null_mut(),
            py_channel_get_balance: std::ptr::null_mut(),
            py_channel_check: std::ptr::null_mut(),
            py_channel_send_throw: std::ptr::null_mut(),
            py_scheduler_get_scheduler: std::ptr::null_mut(),
            py_scheduler_schedule: std::ptr::null_mut(),
            py_scheduler_get_run_count: std::ptr::null_mut(),
            py_scheduler_get_current: std::ptr::null_mut(),
            py_scheduler_run_with_timeout: std::ptr::null_mut(),
            py_scheduler_run_n_tasklets: std::ptr::null_mut(),
            py_scheduler_set_channel_callback: std::ptr::null_mut(),
            py_scheduler_get_channel_callback: std::ptr::null_mut(),
            py_scheduler_set_schedule_callback: std::ptr::null_mut(),
            py_scheduler_set_schedule_fast_callback: std::ptr::null_mut(),
            py_scheduler_get_number_of_active_schedule_managers: std::ptr::null_mut(),
            py_scheduler_get_number_of_active_channels: std::ptr::null_mut(),
            py_scheduler_get_all_time_tasklet_count: std::ptr::null_mut(),
            py_scheduler_get_active_tasklet_count: std::ptr::null_mut(),
            py_scheduler_get_tasklets_completed_last_run_with_timeout: std::ptr::null_mut(),
            py_scheduler_get_tasklets_switched_last_run_with_timeout: std::ptr::null_mut(),
            py_tasklet_type: std::ptr::null_mut(),
            py_channel_type: std::ptr::null_mut(),
            tasklet_exit: std::ptr::null_mut(),
            py_tasklet_get_times_switched_to: std::ptr::null_mut(),
            py_tasklet_get_context: std::ptr::null_mut(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarbonSchedulerStatus {
    Ok = 0,
    InvalidHandle = 1,
    Unsupported = 2,
    Panic = 255,
}

#[no_mangle]
pub extern "C" fn carbon_scheduler_abi_version() -> u32 {
    CARBON_SCHEDULER_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn carbon_scheduler_core_status() -> CarbonSchedulerStatus {
    CarbonSchedulerStatus::Ok
}

pub fn contain_panic<F>(operation: F) -> CarbonSchedulerStatus
where
    F: FnOnce() -> CarbonSchedulerStatus + UnwindSafe,
{
    catch_unwind(operation).unwrap_or(CarbonSchedulerStatus::Panic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{size_of, MaybeUninit};

    macro_rules! offset_of {
        ($field:ident) => {{
            let uninit = MaybeUninit::<SchedulerCapi>::uninit();
            let base = uninit.as_ptr();
            unsafe { std::ptr::addr_of!((*base).$field) as usize - base as usize }
        }};
    }

    #[test]
    fn version_is_stable_for_initial_abi() {
        assert_eq!(carbon_scheduler_abi_version(), 1);
    }

    #[test]
    fn panics_are_mapped_to_status() {
        let status = contain_panic(|| panic!("contained"));
        assert_eq!(status, CarbonSchedulerStatus::Panic);
    }

    #[test]
    fn scheduler_c_api_layout_matches_scheduler_h_order() {
        assert_eq!(SCHEDULER_C_API_CAPSULE_NAME, "scheduler._C_API");
        assert_eq!(SCHEDULER_C_API_CAPSULE_NAME_CSTR, b"scheduler._C_API\0");
        assert_eq!(
            SCHEDULER_C_API_FIELD_NAMES.len(),
            SCHEDULER_C_API_FIELD_COUNT
        );
        assert_eq!(
            size_of::<SchedulerCapi>(),
            SCHEDULER_C_API_FIELD_COUNT * size_of::<*mut c_void>()
        );

        let offsets = [
            offset_of!(py_tasklet_new),
            offset_of!(py_tasklet_setup),
            offset_of!(py_tasklet_insert),
            offset_of!(py_tasklet_get_block_trap),
            offset_of!(py_tasklet_set_block_trap),
            offset_of!(py_tasklet_is_main),
            offset_of!(py_tasklet_check),
            offset_of!(py_tasklet_alive),
            offset_of!(py_tasklet_kill),
            offset_of!(py_channel_new),
            offset_of!(py_channel_send),
            offset_of!(py_channel_receive),
            offset_of!(py_channel_send_exception),
            offset_of!(py_channel_get_queue),
            offset_of!(py_channel_get_preference),
            offset_of!(py_channel_set_preference),
            offset_of!(py_channel_get_balance),
            offset_of!(py_channel_check),
            offset_of!(py_channel_send_throw),
            offset_of!(py_scheduler_get_scheduler),
            offset_of!(py_scheduler_schedule),
            offset_of!(py_scheduler_get_run_count),
            offset_of!(py_scheduler_get_current),
            offset_of!(py_scheduler_run_with_timeout),
            offset_of!(py_scheduler_run_n_tasklets),
            offset_of!(py_scheduler_set_channel_callback),
            offset_of!(py_scheduler_get_channel_callback),
            offset_of!(py_scheduler_set_schedule_callback),
            offset_of!(py_scheduler_set_schedule_fast_callback),
            offset_of!(py_scheduler_get_number_of_active_schedule_managers),
            offset_of!(py_scheduler_get_number_of_active_channels),
            offset_of!(py_scheduler_get_all_time_tasklet_count),
            offset_of!(py_scheduler_get_active_tasklet_count),
            offset_of!(py_scheduler_get_tasklets_completed_last_run_with_timeout),
            offset_of!(py_scheduler_get_tasklets_switched_last_run_with_timeout),
            offset_of!(py_tasklet_type),
            offset_of!(py_channel_type),
            offset_of!(tasklet_exit),
            offset_of!(py_tasklet_get_times_switched_to),
            offset_of!(py_tasklet_get_context),
        ];

        for (index, (name, offset)) in SCHEDULER_C_API_FIELD_NAMES.iter().zip(offsets).enumerate() {
            assert_eq!(
                offset,
                index * size_of::<*mut c_void>(),
                "{name} offset should match Scheduler.h ABI order",
            );
        }
    }
}
