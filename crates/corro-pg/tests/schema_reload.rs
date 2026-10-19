use std::{sync::Arc, time::Duration};

use bytes::Buf;
use corro_agent::agent::util::execute_schema_from_paths;
use corro_pg::{start, PgServer};
use corro_tests::{launch_test_agent, TestAgent};
use corro_types::config::PgConfig;
use futures::{SinkExt, StreamExt};
use pgwire::{
    api::client::auth::DefaultStartupHandler,
    messages::{
        extendedquery::{Bind, Close, Execute, Parse, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL},
        PgWireBackendMessage, PgWireFrontendMessage,
    },
    tokio::client::PgWireClient,
};
use spawn::wait_for_all_pending_handles;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tripwire::Tripwire;

async fn connect(addr: std::net::SocketAddr) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=testuser", addr.ip(), addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    client
}

async fn reload_table(ta: &TestAgent, table: &str) {
    tokio::fs::write(
        ta.tmpdir.path().join("schema").join(format!("{table}.sql")),
        format!("CREATE TABLE {table} (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL DEFAULT '') WITHOUT ROWID;"),
    )
    .await
    .unwrap();
    // The schema-file handler called by the admin Reload command.
    execute_schema_from_paths(&ta.agent).await.unwrap();
    ta.api_client()
        .execute(
            &[format!("INSERT INTO {table} VALUES (1, 'after-reload')")
                .as_str()
                .into()],
            None,
        )
        .await
        .unwrap();
}

async fn simple_count(client: &Client, sql: &str) -> i64 {
    let messages = client.simple_query(sql).await.unwrap();
    let row = messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("expected a count row");
    row.get(0).unwrap().parse().unwrap()
}

async fn setup(tripwire: Tripwire) -> (TestAgent, PgServer) {
    let ta = launch_test_agent(|conf| conf.build(), tripwire.clone())
        .await
        .unwrap();
    let server = start(
        ta.agent.clone(),
        PgConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            tls: None,
            readonly: false,
        },
        tripwire,
    )
    .await
    .unwrap();
    (ta, server)
}

async fn schema_reload(prepared: bool) {
    let (tripwire, worker, shutdown) = Tripwire::new_simple();
    let (ta, server) = setup(tripwire).await;
    let old = connect(server.local_addr).await;
    let mut observed = Vec::new();

    for table in ["late1", "late2"] {
        let sql = format!("SELECT count(*) FROM crsql_changes WHERE \"table\" = '{table}' AND val = 'after-reload'");
        // Prepare and execute before reload, then bind that same statement again.
        let statement = if prepared {
            Some(old.prepare(&sql).await.unwrap())
        } else {
            None
        };
        let before = if let Some(stmt) = &statement {
            old.query_one(stmt, &[]).await.unwrap().get::<_, i64>(0)
        } else {
            simple_count(&old, &sql).await
        };
        assert_eq!(before, 0);
        reload_table(&ta, table).await;

        let fresh = connect(server.local_addr).await;
        assert_eq!(
            simple_count(&fresh, &sql).await,
            1,
            "fresh connection control"
        );
        let count = if let Some(stmt) = &statement {
            old.query_one(stmt, &[]).await.unwrap().get::<_, i64>(0)
        } else {
            simple_count(&old, &sql).await
        };
        // The old connection sees committed user data, not an old WAL snapshot.
        assert_eq!(
            simple_count(
                &old,
                &format!("SELECT count(*) FROM {table} WHERE value = 'after-reload'")
            )
            .await,
            1
        );
        observed.push(count);
        drop(fresh);
    }

    // An unchanged table checks that refresh preserves subsequent write tracking.
    old.batch_execute("INSERT INTO tests (id, text) VALUES (99, 'pg-write')")
        .await
        .unwrap();
    let fresh = connect(server.local_addr).await;
    assert_eq!(
        simple_count(
            &fresh,
            "SELECT count(*) FROM crsql_changes WHERE \"table\" = 'tests' AND val = 'pg-write'"
        )
        .await,
        1
    );
    drop(fresh);
    drop(old);
    shutdown.send(()).await.ok();
    worker.await;
    wait_for_all_pending_handles().await;
    assert_eq!(
        observed,
        [1, 1],
        "existing session must discover both reloaded tables"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_reload_simple_query() {
    schema_reload(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_reload_prepared_statement() {
    schema_reload(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_reload_waits_for_transaction() {
    let (tripwire, worker, shutdown) = Tripwire::new_simple();
    let (ta, server) = setup(tripwire).await;
    let mut old = connect(server.local_addr).await;
    let sql = "SELECT count(*) FROM crsql_changes WHERE \"table\" = 'during_tx'";
    let tx = old.transaction().await.unwrap();
    assert_eq!(tx.query_one(sql, &[]).await.unwrap().get::<_, i64>(0), 0);
    reload_table(&ta, "during_tx").await;
    // The active transaction retains its snapshot; refresh is deferred.
    assert_eq!(tx.query_one(sql, &[]).await.unwrap().get::<_, i64>(0), 0);
    tx.commit().await.unwrap();
    let after = simple_count(&old, sql).await;
    drop(old);
    shutdown.send(()).await.ok();
    worker.await;
    wait_for_all_pending_handles().await;
    assert_eq!(after, 1);
}

async fn roundtrip(
    client: &mut PgWireClient,
    messages: Vec<PgWireFrontendMessage>,
) -> Vec<PgWireBackendMessage> {
    for message in messages {
        client.send(message).await.unwrap();
    }
    client
        .send(PgWireFrontendMessage::Sync(PgSync::new()))
        .await
        .unwrap();
    let mut responses = Vec::new();
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("PostgreSQL response timed out")
            .expect("PostgreSQL connection closed")
            .unwrap();
        assert!(
            !matches!(message, PgWireBackendMessage::ErrorResponse(_)),
            "{message:?}"
        );
        let ready = matches!(message, PgWireBackendMessage::ReadyForQuery(_));
        responses.push(message);
        if ready {
            return responses;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_reload_waits_for_suspended_cursor() {
    let (tripwire, worker, shutdown) = Tripwire::new_simple();
    let (ta, server) = setup(tripwire).await;
    reload_table(&ta, "cursor_source").await;
    ta.api_client()
        .execute(
            &["INSERT INTO cursor_source VALUES (2, 'second')".into()],
            None,
        )
        .await
        .unwrap();

    let config = Arc::new(
        format!(
            "host={} port={} user=testuser sslmode=disable",
            server.local_addr.ip(),
            server.local_addr.port()
        )
        .parse()
        .unwrap(),
    );
    let mut client = PgWireClient::connect(config, DefaultStartupHandler::new(), None)
        .await
        .unwrap();
    roundtrip(
        &mut client,
        vec![
            PgWireFrontendMessage::Parse(Parse::new(
                Some("changes".into()),
                "SELECT val FROM crsql_changes WHERE \"table\" = 'cursor_source'".into(),
                vec![],
            )),
            PgWireFrontendMessage::Bind(Bind::new(
                Some("cursor".into()),
                Some("changes".into()),
                vec![],
                vec![],
                vec![0],
            )),
        ],
    )
    .await;

    let rows = roundtrip(
        &mut client,
        vec![PgWireFrontendMessage::Execute(Execute::new(
            Some("cursor".into()),
            1,
        ))],
    )
    .await;
    assert!(rows
        .iter()
        .any(|m| matches!(m, PgWireBackendMessage::PortalSuspended(_))));
    assert_eq!(
        rows.iter()
            .filter(|m| matches!(m, PgWireBackendMessage::DataRow(_)))
            .count(),
        1
    );

    reload_table(&ta, "during_cursor").await;
    // Parse reaches the idle refresh check while the cursor still owns state.
    roundtrip(
        &mut client,
        vec![PgWireFrontendMessage::Parse(Parse::new(
            Some("probe".into()),
            "SELECT count(*) FROM crsql_changes WHERE \"table\" = 'during_cursor'".into(),
            vec![],
        ))],
    )
    .await;
    let remaining = roundtrip(
        &mut client,
        vec![PgWireFrontendMessage::Execute(Execute::new(
            Some("cursor".into()),
            0,
        ))],
    )
    .await;
    assert_eq!(
        remaining
            .iter()
            .filter(|m| matches!(m, PgWireBackendMessage::DataRow(_)))
            .count(),
        1
    );
    roundtrip(
        &mut client,
        vec![PgWireFrontendMessage::Close(Close::new(
            TARGET_TYPE_BYTE_PORTAL,
            Some("cursor".into()),
        ))],
    )
    .await;

    let responses = roundtrip(
        &mut client,
        vec![
            PgWireFrontendMessage::Bind(Bind::new(
                Some("probe_cursor".into()),
                Some("probe".into()),
                vec![],
                vec![],
                vec![0],
            )),
            PgWireFrontendMessage::Execute(Execute::new(Some("probe_cursor".into()), 0)),
        ],
    )
    .await;
    let mut rows = responses.into_iter().filter_map(|m| match m {
        PgWireBackendMessage::DataRow(row) => Some(row),
        _ => None,
    });
    let mut row = rows.next().expect("missing count");
    assert!(rows.next().is_none());
    assert_eq!(row.field_count, 1);
    let length = row.data.get_i32();
    assert!(length >= 0);
    let count = row.data.copy_to_bytes(length as usize);
    drop(client);
    shutdown.send(()).await.ok();
    worker.await;
    wait_for_all_pending_handles().await;
    assert_eq!(count.as_ref(), b"1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_reload_after_bind() {
    let (tripwire, worker, shutdown) = Tripwire::new_simple();
    let (ta, server) = setup(tripwire).await;
    let config = Arc::new(
        format!(
            "host={} port={} user=testuser sslmode=disable",
            server.local_addr.ip(),
            server.local_addr.port()
        )
        .parse()
        .unwrap(),
    );
    let mut client = PgWireClient::connect(config, DefaultStartupHandler::new(), None)
        .await
        .unwrap();
    let sql = "SELECT count(*) FROM crsql_changes WHERE \"table\" = 'after_bind'";
    roundtrip(
        &mut client,
        vec![
            PgWireFrontendMessage::Parse(Parse::new(Some("probe".into()), sql.into(), vec![])),
            PgWireFrontendMessage::Bind(Bind::new(
                Some("prime".into()),
                Some("probe".into()),
                vec![],
                vec![],
                vec![0],
            )),
            PgWireFrontendMessage::Execute(Execute::new(Some("prime".into()), 0)),
            PgWireFrontendMessage::Bind(Bind::new(
                Some("waiting".into()),
                Some("probe".into()),
                vec![],
                vec![],
                vec![0],
            )),
        ],
    )
    .await;
    reload_table(&ta, "after_bind").await;
    let responses = roundtrip(
        &mut client,
        vec![PgWireFrontendMessage::Execute(Execute::new(
            Some("waiting".into()),
            0,
        ))],
    )
    .await;
    let mut row = responses
        .into_iter()
        .find_map(|m| match m {
            PgWireBackendMessage::DataRow(row) => Some(row),
            _ => None,
        })
        .expect("missing count");
    assert_eq!(row.field_count, 1);
    let length = row.data.get_i32();
    assert!(length >= 0);
    let count = row.data.copy_to_bytes(length as usize);
    drop(client);
    shutdown.send(()).await.ok();
    worker.await;
    wait_for_all_pending_handles().await;
    assert_eq!(count.as_ref(), b"1");
}
