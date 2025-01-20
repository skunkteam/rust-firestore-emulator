use std::sync::Mutex;

use axum::async_trait;
use tonic::Response;

use crate::common::proto::{test1_server::*, *};

pub struct Test1Service {
    pub state: Mutex<u32>,
}

#[async_trait]
impl Test1 for Test1Service {
    async fn test1(
        &self,
        _request: tonic::Request<super::proto::Test1Request>,
    ) -> Result<tonic::Response<super::proto::Test1Reply>, tonic::Status> {
        *self.state.lock().unwrap() += 5;

        println!("{}", self.state.lock().unwrap().clone());

        Ok(Response::new(Test1Reply {}))
    }
}
