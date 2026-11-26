# Storage

Recordings go to storage you control. Five backends ship, they are added through
**Settings** in the running application, and every one of them is tested — written
to, read back, deleted — before it is saved. A backend that cannot be written to is
rejected there rather than in the middle of somebody's recording.

Credentials are encrypted at rest with `SECRET_KEY` and no endpoint returns them,
not even to an administrator. They go in, they are used, that is all.

## What each one needs

### Local disk

| Field | Example |
|---|---|
| Directory | `./data/storage` |

Fine for one server. The API and the worker must see the same path — under compose
they share a volume for exactly this reason — and nothing else can read the files,
so playback is served by the API through signed URLs.

### S3 or compatible

AWS S3, MinIO, Cloudflare R2, Backblaze B2.

| Field | Example |
|---|---|
| Bucket | `osprey` |
| Region | `us-east-1` |
| Endpoint | blank for AWS, `http://minio:9000` for MinIO in compose |
| Endpoint browsers use | only if browsers reach it at a different address |
| Access key id, Secret access key | |

The last field is the one that catches people. Inside compose the API reaches MinIO
at `http://minio:9000`, but a browser cannot resolve `minio` — it needs
`http://localhost:9000`. Set both and the URLs are signed for the right host each
time.

This is the only backend a browser uploads to directly.

### Azure Blob Storage

| Field | Example |
|---|---|
| Storage account name | `mystorageaccount` (3–24 lowercase letters or digits) |
| Container | `osprey-recordings` |
| Blob endpoint | `https://mystorageaccount.blob.core.windows.net` (optional) |
| Endpoint browsers use | only when browsers reach the account at a different address (optional) |
| Storage account key | Base64 account key from Azure |

The server publishes completed uploads as block blobs. The API and worker must
share the staging directory; multiple API instances need the same shared path.
Playback uses short-lived, read-only SAS URLs. When the browser-facing endpoint
differs from the server endpoint, set **Endpoint browsers use** so SAS URLs point
to an address the browser can reach.

### Cloudinary

| Field | Example |
|---|---|
| Cloud name | from the dashboard, not your account email |
| Folder | `osprey` |
| API key, API secret | |

### ImageKit

| Field | Example |
|---|---|
| URL endpoint | `https://ik.imagekit.io/your-id` |
| Public key | `public_…` |
| Folder | `osprey` |
| Private key | |

## What they can actually do

Declared capabilities are checked against measured behaviour by a shared conformance
suite, so this table is what the providers do rather than what they claim.

| | S3/MinIO | Azure Blob | Cloudinary | ImageKit | Local disk |
|---|---|---|---|---|---|
| Browser uploads directly | yes | no, staged | no, staged | no, staged | no |
| Multipart, parallel | yes, native | yes, into staging | yes, into staging | yes, into staging | yes |
| Resumable | yes | yes | yes | yes | yes |
| Signed reads | yes | yes | yes | yes | yes |
| Byte ranges | yes | yes | yes | yes | yes |
| Immediately consistent | yes | yes | yes | **no** | yes |
| Returns the exact bytes given | yes | yes | yes | **only with `orig-true`** | yes |
| Accepts non-media bytes | yes | yes | **no** | serves them oddly | yes |
| Transcodes for you | no | no | yes | yes, on the fly | no |
| Adaptive streaming | no | no | HLS + DASH | HLS + DASH | no |
| Smallest part | 5 MiB | 1 byte | 1 byte | 1 byte | 1 byte |

The provider-specific details worth calling out are:

**Azure Blob Storage uses staged block-blob uploads.** Parts are assembled on the server, then uploaded in batches of up to four 8 MiB blocks. The 50,000-block limit caps an object at about 419 GB (391 GiB). Account keys stay in the encrypted secret field, and playback links use read-only, time-limited SAS tokens.

**Cloudinary rejects non-media uploads outright.** The connection test and the
conformance suite both have to send real video to it, not random bytes.

**ImageKit serves an optimised rendition by default**, which is not the file you
uploaded. Reads add `?tr=orig-true` to get the original back. Its index is also
eventually consistent, so `stat` goes through an HTTP HEAD and deletes retry.

**Azure Blob Storage, Cloudinary and ImageKit all use the shared staging directory.** Azure publishes the assembled file as blocks; Cloudinary and ImageKit publish it as one object. The directory is fixed because parts arrive across separate requests and each request builds its own connector. Running more than one API process requires that directory on shared storage.

## Adding a backend

Implement `StorageConnector` from `packages/storage/src/types.ts` and run the
conformance suite against it:

```ts
runConformanceSuite('your backend', async () => ({
  connector: makeYourConnector(),
  fresh: () => makeYourConnector(),
  cleanup: async () => {
    /* remove whatever the test left behind */
  },
}));
```

The suite is what makes "pluggable storage" a property of the system rather than an
intention. A backend is finished when it passes.

Two things it checks that are easy to miss. It rebuilds the connector **between
every call**, because the API constructs one per request and a connector that keeps
upload state in a field works perfectly in a test that reuses one object and fails
the moment it is deployed — that exact bug shipped once. And it checks declared
capabilities against what the backend actually did, so claiming
`immediatelyConsistent` when you are not fails the suite rather than producing a
subtle bug months later.
