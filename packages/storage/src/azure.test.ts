import { randomBytes } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { AzureBlobPublisher } from './azure.ts';
import { buildConnector } from './factory.ts';

const accountKey = Buffer.from('0123456789abcdef0123456789abcdef').toString('base64');
const publisherOptions = {
  accountName: 'exampleaccount',
  accountKey,
  container: 'osprey-recordings',
};

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('Azure Blob Storage playback URLs', () => {
  it('buckets SAS expiry ahead and matches a known signature', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-01T00:00:00.000Z'));

    const target = await new AzureBlobPublisher(publisherOptions).playbackUrl(
      'r/abc/original.mp4',
      { ttlSeconds: 60 },
    );
    const url = new URL(target.url);

    expect(target.kind).toBe('progressive');
    expect(url.searchParams.get('sp')).toBe('r');
    expect(url.searchParams.get('spr')).toBe('https');
    expect(url.searchParams.get('sr')).toBe('b');
    expect(url.searchParams.get('sv')).toBe('2023-11-03');
    expect(url.searchParams.get('se')).toBe('2024-01-01T00:02:00Z');
    expect(url.searchParams.get('sig')).toBe(
      'oYotOMrfoMEE4TsKbhgyjFCcDZzjq7Wh1Sg+tqDYlNU=',
    );
    expect(target.expiresAt?.getTime()).toBe(Date.parse('2024-01-01T00:02:00Z'));
    expect(target.expiresAt!.getTime() - Date.now()).toBeGreaterThan(60_000);
  });

  it('uses an explicit browser endpoint for playback URLs', async () => {
    const publisher = new AzureBlobPublisher({
      ...publisherOptions,
      endpoint: 'https://internal.example.test',
      publicEndpoint: 'https://media.example.test/azure',
    });
    const target = await publisher.playbackUrl('r/abc/original.mp4', { ttlSeconds: 60 });
    const url = new URL(target.url);

    expect(url.origin).toBe('https://media.example.test');
    expect(url.pathname).toBe('/azure/osprey-recordings/r/abc/original.mp4');
    expect(url.searchParams.get('spr')).toBe('https');
  });

  it('allows HTTP-compatible endpoints with an HTTP-capable SAS protocol', async () => {
    const publisher = new AzureBlobPublisher({
      ...publisherOptions,
      endpoint: 'http://127.0.0.1:10000/devstoreaccount1',
      accountName: 'devstoreaccount1',
    });
    const target = await publisher.playbackUrl('r/abc/original.mp4', { ttlSeconds: 60 });
    const url = new URL(target.url);

    expect(url.protocol).toBe('http:');
    expect(url.pathname).toBe('/devstoreaccount1/osprey-recordings/r/abc/original.mp4');
    expect(url.searchParams.get('spr')).toBe('https,http');
  });

  it('matches Azure Shared Key authorization for a known request', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-01T00:00:00.000Z'));

    const fetchMock = vi.fn(
      async (..._args: Parameters<typeof fetch>) =>
        new Response(null, { status: 404 }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await expect(
      new AzureBlobPublisher(publisherOptions).stat('r/abc/original.mp4'),
    ).resolves.toBeNull();
    const request = fetchMock.mock.calls[0]?.[1];

    expect(new Headers(request?.headers).get('authorization')).toBe(
      'SharedKey exampleaccount:LiR8z9uIDOSr4UzteyiQaDStDCQR5XNpeEPvy8Q2R4M=',
    );
  });

  it('includes the account twice in emulator-style Shared Key signatures', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-01T00:00:00.000Z'));

    const fetchMock = vi.fn(
      async (..._args: Parameters<typeof fetch>) => new Response(null, { status: 404 }),
    );
    vi.stubGlobal('fetch', fetchMock);

    const publisher = new AzureBlobPublisher({
      ...publisherOptions,
      accountName: 'devstoreaccount1',
      endpoint: 'http://127.0.0.1:10000/devstoreaccount1',
    });
    await expect(publisher.stat('r/blob.mp4')).resolves.toBeNull();
    const request = fetchMock.mock.calls[0]?.[1];

    expect(new Headers(request?.headers).get('authorization')).toBe(
      'SharedKey devstoreaccount1:l40N2B4b3JTYPbCsTnJK/TiAvfQEz733IlcrL1TZ5jM=',
    );
  });

  it('uses Put Blob rather than staging an invalid zero-length block', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'osprey-azure-test-'));
    const localPath = join(directory, 'empty.bin');
    await writeFile(localPath, Buffer.alloc(0));

    const fetchMock = vi.fn(
      async (..._args: Parameters<typeof fetch>) =>
        new Response(null, { status: 201 }),
    );
    vi.stubGlobal('fetch', fetchMock);

    try {
      const publisher = new AzureBlobPublisher(publisherOptions);
      await expect(
        publisher.publish({
          localPath,
          objectKey: 'r/empty.bin',
          contentType: 'application/octet-stream',
        }),
      ).resolves.toEqual({ bytes: 0 });

      expect(fetchMock).toHaveBeenCalledTimes(1);
      const request = fetchMock.mock.calls[0]?.[1];
      const headers = new Headers(request?.headers);

      expect(request?.method).toBe('PUT');
      expect(headers.get('x-ms-blob-type')).toBe('BlockBlob');
      expect(headers.get('content-length')).toBe('0');
      expect(request?.body).toBeUndefined();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});


describe('Azure Blob Storage factory configuration', () => {
  it('passes the browser-facing endpoint from stored config to playback URLs', async () => {
    const connector = buildConnector(
      {
        kind: 'azure',
        config: {
          accountName: 'exampleaccount',
          container: 'osprey-recordings',
          endpoint: 'https://internal.example.test',
          publicEndpoint: 'https://media.example.test/azure',
        },
        secret: { accountKey },
      },
      {
        localBaseUrl: 'http://localhost',
        signingSecret: 'test-secret',
        stagingRoot: '/tmp/osprey-azure-factory-test',
      },
    );

    const target = await connector.getPlaybackTarget('r/abc/original.mp4', { ttlSeconds: 60 });
    const url = new URL(target.url);

    expect(url.origin).toBe('https://media.example.test');
    expect(url.pathname).toBe('/azure/osprey-recordings/r/abc/original.mp4');
    expect(url.searchParams.get('spr')).toBe('https');
  });
});

const azurite = {
  accountName: process.env.AZURE_TEST_ACCOUNT_NAME,
  accountKey: process.env.AZURE_TEST_ACCOUNT_KEY,
  container: process.env.AZURE_TEST_CONTAINER,
  endpoint: process.env.AZURE_TEST_ENDPOINT,
  publicEndpoint: process.env.AZURE_TEST_PUBLIC_ENDPOINT,
};

if (azurite.accountName && azurite.accountKey && azurite.container && azurite.endpoint) {
  describe('Azure Blob Storage Azurite integration', () => {
    it('serves an uploaded object through its signed browser playback URL', async () => {
      const directory = await mkdtemp(join(tmpdir(), 'osprey-azure-azurite-'));
      const localPath = join(directory, 'sample.bin');
      const objectKey = 'osprey-sas-test/' + randomBytes(8).toString('hex') + '.bin';
      const body = Buffer.from('Azurite verifies the Azure Shared Key and SAS signatures.');
      await writeFile(localPath, body);

      const publisher = new AzureBlobPublisher({
        accountName: azurite.accountName!,
        accountKey: azurite.accountKey!,
        container: azurite.container!,
        endpoint: azurite.endpoint,
        publicEndpoint: azurite.publicEndpoint,
      });

      try {
        await publisher.ensureContainerExists();
        await publisher.publish({ localPath, objectKey, contentType: 'application/octet-stream' });
        const target = await publisher.playbackUrl(objectKey, { ttlSeconds: 60 });
        const response = await fetch(target.url);

        expect(response.ok).toBe(true);
        expect(Buffer.from(await response.arrayBuffer()).equals(body)).toBe(true);
      } finally {
        await publisher.remove(objectKey).catch(() => undefined);
        await rm(directory, { recursive: true, force: true });
      }
    });
  });
} else {
  describe.skip('Azure Blob Storage Azurite integration (set AZURE_TEST_* to run)', () => {});
}
