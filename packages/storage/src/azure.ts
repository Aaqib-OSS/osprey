import { createHmac } from 'node:crypto';
import { open, stat as statFile } from 'node:fs/promises';
import { Readable } from 'node:stream';

import { StorageError } from './errors.ts';
import { assertSafeKey } from './keys.ts';
import type { ByteRange, ObjectStat, PlaybackTarget } from './types.ts';
import type { Publisher } from './staged.ts';

const API_VERSION = '2023-11-03';
const BLOCK_BYTES = 8 * 1024 * 1024;
const MAX_BLOCKS = 50_000;
const MAX_PARALLEL_BLOCKS = 4;

export interface AzureBlobOptions {
  accountName: string;
  accountKey: string;
  container: string;
  /** Override for Azure-compatible endpoints and private clouds. */
  endpoint?: string;
  /** Endpoint the browser can reach, when it differs from the server's endpoint. */
  publicEndpoint?: string;
}

/**
 * Azure Blob Storage publisher. The connector stages upload parts locally and
 * publishes completed files as a sequence of blocks, so no upload state lives on
 * this instance and files larger than a single request stay bounded in memory.
 */
export class AzureBlobPublisher implements Publisher {
  readonly kind = 'azure' as const;
  readonly delivery = {
    signedRead: true,
    rangeRequests: true,
    serverSideTranscode: false,
    adaptiveStreaming: false,
    immediatelyConsistent: true,
    maxObjectBytes: BLOCK_BYTES * MAX_BLOCKS,
  } as const;

  private readonly accountName: string;
  private readonly accountKey: Buffer;
  private readonly container: string;
  private readonly endpoint: string;
  private readonly publicEndpoint: string;

  constructor(options: AzureBlobOptions) {
    const endpoint = options.endpoint ?? 'https://' + options.accountName + '.blob.core.windows.net';
    const publicEndpoint = options.publicEndpoint ?? endpoint;
    const endpointUrl = new URL(endpoint);
    const publicEndpointUrl = new URL(publicEndpoint);
    const validEndpoint = (url: URL) =>
      (url.protocol === 'http:' || url.protocol === 'https:') && !url.search && !url.hash;

    if (!/^[a-z0-9]{3,24}$/.test(options.accountName)) {
      throw new StorageError(
        'PROVIDER_ERROR',
        'Azure storage account names must be 3 to 24 lowercase letters or numbers.',
      );
    }
    if (
      !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(options.container) ||
      options.container.length < 3 ||
      options.container.length > 63
    ) {
      throw new StorageError(
        'PROVIDER_ERROR',
        'Azure container names must be 3 to 63 lowercase letters, numbers, or single hyphens between segments.',
      );
    }
    if (!validEndpoint(endpointUrl) || !validEndpoint(publicEndpointUrl)) {
      throw new StorageError(
        'PROVIDER_ERROR',
        'Azure endpoints must be HTTP or HTTPS URLs without a query or fragment.',
      );
    }

    this.accountName = options.accountName;
    this.accountKey = Buffer.from(options.accountKey, 'base64');
    this.container = options.container;
    this.endpoint = endpoint.replace(/\/+$/, '');
    this.publicEndpoint = publicEndpoint.replace(/\/+$/, '');
    if (this.accountKey.length === 0) {
      throw new StorageError('PROVIDER_ERROR', 'An Azure base64 account key is required.');
    }
  }

  /** Create the configured container when a test or deployment needs to provision it. */
  async ensureContainerExists(): Promise<void> {
    const url = new URL(this.blobUrl(''));
    url.searchParams.set('restype', 'container');
    await this.send(url, 'PUT', {
      headers: { 'Content-Length': '0' },
      allowConflict: true,
    });
  }

  async publish(input: {
    localPath: string;
    objectKey: string;
    contentType: string;
  }): Promise<{ bytes: number }> {
    assertSafeKey(input.objectKey);
    const info = await statFile(input.localPath);
    if (info.size === 0) {
      await this.request(input.objectKey, 'PUT', {
        headers: {
          'Content-Length': '0',
          'Content-Type': input.contentType,
          'x-ms-blob-type': 'BlockBlob',
        },
      });
      return { bytes: 0 };
    }

    const count = Math.ceil(info.size / BLOCK_BYTES);
    if (count > MAX_BLOCKS) {
      throw new StorageError(
        'PROVIDER_ERROR',
        'Azure Blob Storage supports at most ' + MAX_BLOCKS + ' blocks per object.',
      );
    }

    const handle = await open(input.localPath, 'r');
    const blockIds = new Array<string>(count);
    try {
      for (let first = 0; first < count; first += MAX_PARALLEL_BLOCKS) {
        const indices = Array.from(
          { length: Math.min(MAX_PARALLEL_BLOCKS, count - first) },
          (_, offset) => first + offset,
        );
        const results = await Promise.allSettled(indices.map(async (index) => {
          const length = Math.min(BLOCK_BYTES, info.size - index * BLOCK_BYTES);
          const buffer = Buffer.alloc(length);
          let offset = 0;
          while (offset < length) {
            const { bytesRead } = await handle.read(
              buffer,
              offset,
              length - offset,
              index * BLOCK_BYTES + offset,
            );
            if (bytesRead === 0) {
              throw new StorageError(
                'PROVIDER_ERROR',
                'The staged Azure upload changed while it was being read.',
              );
            }
            offset += bytesRead;
          }

          const blockId = Buffer.from(String(index).padStart(8, '0')).toString('base64');
          await this.request(input.objectKey, 'PUT', {
            query: { comp: 'block', blockid: blockId },
            body: buffer,
            headers: { 'Content-Length': String(length) },
          });
          return { index, blockId };
        }));

        for (const result of results) {
          if (result.status === 'rejected') throw result.reason;
          blockIds[result.value.index] = result.value.blockId;
        }
      }
    } finally {
      await handle.close();
    }

    const xml = '<?xml version="1.0" encoding="utf-8"?><BlockList>' +
      blockIds.map((id) => '<Latest>' + id + '</Latest>').join('') +
      '</BlockList>';
    await this.request(input.objectKey, 'PUT', {
      query: { comp: 'blocklist' },
      body: Buffer.from(xml),
      headers: {
        'Content-Type': 'application/xml',
        'x-ms-blob-content-type': input.contentType,
      },
    });
    return { bytes: info.size };
  }

  async stat(objectKey: string): Promise<ObjectStat | null> {
    assertSafeKey(objectKey);
    const response = await this.request(objectKey, 'HEAD', { allowNotFound: true });
    if (response.status === 404) return null;
    return {
      bytes: Number(response.headers.get('content-length') ?? 0),
      contentType: response.headers.get('content-type') ?? 'application/octet-stream',
    };
  }

  async remove(objectKey: string): Promise<void> {
    assertSafeKey(objectKey);
    await this.request(objectKey, 'DELETE', { allowNotFound: true });
  }

  async playbackUrl(objectKey: string, options: { ttlSeconds: number }): Promise<PlaybackTarget> {
    assertSafeKey(objectKey);
    const ttl = Math.max(1, Math.floor(options.ttlSeconds));
    const bucket = Math.floor(Date.now() / 1000 / ttl);
    const expiresAtSeconds = (bucket + 2) * ttl;
    const expires = new Date(expiresAtSeconds * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
    const protocol = new URL(this.publicEndpoint).protocol === 'https:' ? 'https' : 'https,http';
    const resource = '/blob/' + this.accountName + '/' + this.container + '/' + objectKey;
    const stringToSign = [
      'r', '', expires, resource, '', '', protocol, API_VERSION, 'b', '', '', '', '', '', '', '',
    ].join('\n');
    const signature = createHmac('sha256', this.accountKey).update(stringToSign, 'utf8').digest('base64');
    const url = new URL(this.blobUrl(objectKey, this.publicEndpoint));
    url.searchParams.set('sp', 'r');
    url.searchParams.set('se', expires);
    url.searchParams.set('spr', protocol);
    url.searchParams.set('sv', API_VERSION);
    url.searchParams.set('sr', 'b');
    url.searchParams.set('sig', signature);
    return { url: url.toString(), kind: 'progressive', expiresAt: new Date(expiresAtSeconds * 1000) };
  }

  async openRead(objectKey: string, range?: ByteRange): Promise<Readable> {
    assertSafeKey(objectKey);
    const response = await this.request(objectKey, 'GET', {
      headers: range ? { Range: 'bytes=' + range.start + '-' + (range.end ?? '') } : {},
      allowNotFound: true,
    });
    if (response.status === 404) {
      throw new StorageError('NOT_FOUND', 'No object at ' + objectKey + '.');
    }
    if (!response.body) {
      throw new StorageError('PROVIDER_ERROR', 'Azure returned an empty response body.');
    }
    return Readable.fromWeb(response.body as import('node:stream/web').ReadableStream);
  }

  private blobUrl(objectKey: string, endpoint = this.endpoint): string {
    const encoded = objectKey.split('/').map(encodeURIComponent).join('/');
    return endpoint + '/' + encodeURIComponent(this.container) + (encoded ? '/' + encoded : '');
  }

  private async request(
    objectKey: string,
    method: string,
    options: {
      query?: Record<string, string>;
      headers?: Record<string, string>;
      body?: Buffer;
      allowNotFound?: boolean;
      allowConflict?: boolean;
    } = {},
  ): Promise<Response> {
    const url = new URL(this.blobUrl(objectKey));
    for (const [key, value] of Object.entries(options.query ?? {})) url.searchParams.set(key, value);
    return this.send(url, method, options);
  }

  private async send(
    url: URL,
    method: string,
    options: {
      headers?: Record<string, string>;
      body?: Buffer;
      allowNotFound?: boolean;
      allowConflict?: boolean;
    } = {},
  ): Promise<Response> {
    const headers: Record<string, string> = {
      'x-ms-date': new Date().toUTCString(),
      'x-ms-version': API_VERSION,
      ...options.headers,
    };
    const body = options.body;
    if (body !== undefined && !headers['Content-Length']) {
      headers['Content-Length'] = String(body.byteLength);
    }
    headers.Authorization = this.authorization(method, url, headers, body?.byteLength ?? 0);

    const response = await fetch(url, { method, headers, body });
    if (options.allowNotFound && response.status === 404) return response;
    if (options.allowConflict && response.status === 409) return response;
    if (!response.ok) {
      const detail = (await response.text()).slice(0, 500);
      if (response.status === 404) {
        throw new StorageError('NOT_FOUND', 'No object at ' + url.pathname + '.');
      }
      throw new StorageError(
        'PROVIDER_ERROR',
        'Azure Blob Storage returned HTTP ' + response.status + (detail ? ': ' + detail : ''),
        { retryable: response.status >= 500 || response.status === 429 },
      );
    }
    return response;
  }

  private authorization(
    method: string,
    url: URL,
    headers: Record<string, string>,
    contentLength: number,
  ): string {
    const normalized = Object.entries(headers)
      .filter(([name]) => name.toLowerCase().startsWith('x-ms-'))
      .map(([name, value]) => [name.toLowerCase(), value.trim().replace(/\s+/g, ' ')] as const)
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([name, value]) => name + ':' + value + '\n')
      .join('');

    // Azure's Shared Key canonicalized resource always starts with the account name.
    // IP-style Azurite endpoints also include it in the URL path, so it appears twice.
    const resource = '/' + this.accountName + url.pathname;
    const queryValues = new Map<string, string[]>();
    for (const [name, value] of url.searchParams.entries()) {
      const key = name.toLowerCase();
      const values = queryValues.get(key) ?? [];
      values.push(value);
      queryValues.set(key, values);
    }
    const canonicalizedQuery = [...queryValues.entries()]
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([name, values]) => '\n' + name + ':' + values.sort().join(','))
      .join('');
    const canonicalResource = resource + canonicalizedQuery;
    const stringToSign = [
      method, '', '', contentLength === 0 ? '' : String(contentLength), '',
      headers['Content-Type'] ?? '', '', '', '', '', '', headers.Range ?? '',
      normalized + canonicalResource,
    ].join('\n');
    const signature = createHmac('sha256', this.accountKey).update(stringToSign, 'utf8').digest('base64');
    return 'SharedKey ' + this.accountName + ':' + signature;
  }
}
