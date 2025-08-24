use std::thread::{self, JoinHandle};

pub trait TryJoin<T> {
    fn try_join(&mut self) -> Option<thread::Result<T>>;
}

impl<T> TryJoin<T> for Option<JoinHandle<T>> {
    fn try_join(&mut self) -> Option<thread::Result<T>> {
        self.take_if(|join_handle| join_handle.is_finished())
            .map(JoinHandle::join)
    }
}
