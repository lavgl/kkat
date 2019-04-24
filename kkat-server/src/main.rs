use actix_web::{error, server, App, HttpResponse, Query, Responder};
use futures::stream::Stream;

mod kafka;
mod params;

use params::QueryParams;

fn index(query_params: Query<QueryParams>) -> impl Responder {
    println!("query params: {:?}", query_params);
    let receiver = kafka::read_messages(query_params.into_inner())
        .inspect(|v| println!("receiver: {:?}", v))
        .map_err(|_| error::ErrorBadRequest("bad request"));

    HttpResponse::Ok().chunked().streaming(receiver)
}

fn main() {
    // TODO: add 404 (& wrong params) handler
    // TODO: add args for setting bind address, kafka server address
    server::new(|| App::new().resource("/", |r| r.with(index)))
        .bind("127.0.0.1:3000")
        .unwrap()
        .run()
}
