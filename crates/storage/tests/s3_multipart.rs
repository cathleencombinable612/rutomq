use bytes::Bytes;
use rutomq_storage::{
    MIN_S3_MULTIPART_CHUNK_BYTES, ObjectStore, OpenDalObjectStore, S3Config, StorageError,
};
use uuid::Uuid;

#[tokio::test]
async fn minio_multipart_write_is_exact_and_immutable() {
    let Ok(endpoint) = std::env::var("RUTOMQ_TEST_S3_ENDPOINT") else {
        return;
    };
    let config = S3Config {
        bucket: env_or("RUTOMQ_TEST_S3_BUCKET", "rutomq"),
        root: "multipart-test".to_owned(),
        endpoint: Some(endpoint),
        access_key_id: Some(env_or("RUTOMQ_TEST_S3_ACCESS_KEY_ID", "minioadmin")),
        secret_access_key: Some(env_or("RUTOMQ_TEST_S3_SECRET_ACCESS_KEY", "minioadmin")),
        write_chunk_bytes: MIN_S3_MULTIPART_CHUNK_BYTES,
        write_concurrency: 2,
        ..S3Config::default()
    };
    let store = OpenDalObjectStore::s3(config.clone()).unwrap();
    let key = format!("objects/{}.bin", Uuid::new_v4());
    let size = MIN_S3_MULTIPART_CHUNK_BYTES * 2 + 257;
    let payload = Bytes::from(
        (0..size)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>(),
    );

    let metadata = store.put_immutable(&key, payload.clone()).await.unwrap();
    assert_eq!(metadata.size, size as u64);
    assert!(
        metadata
            .etag
            .as_deref()
            .is_some_and(|etag| etag.contains('-')),
        "MinIO multipart ETag was missing: {:?}",
        metadata.etag
    );
    let boundary = MIN_S3_MULTIPART_CHUNK_BYTES as u64;
    assert_eq!(
        store
            .get_range(&key, boundary - 31..boundary + 31)
            .await
            .unwrap(),
        payload.slice(MIN_S3_MULTIPART_CHUNK_BYTES - 31..MIN_S3_MULTIPART_CHUNK_BYTES + 31)
    );
    let second_agent = OpenDalObjectStore::s3(config).unwrap();
    assert!(matches!(
        second_agent
            .put_immutable(&key, Bytes::from_static(b"replacement"))
            .await,
        Err(StorageError::AlreadyExists(_))
    ));
    assert_eq!(
        store.get_range(&key, 0..257).await.unwrap(),
        payload.slice(0..257)
    );
    assert_eq!(
        second_agent.get_range(&key, 0..257).await.unwrap(),
        payload.slice(0..257)
    );
    assert_eq!(store.head(&key).await.unwrap().etag, metadata.etag);

    store.delete(&key).await.unwrap();
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}
