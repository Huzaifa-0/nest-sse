import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { request as httpRequest, type ClientRequest, type IncomingMessage } from 'node:http';
import { afterAll, afterEach, beforeAll, describe, expect, it } from '@jest/globals';
import request from 'supertest';
import { AppModule } from '../src/demo/app.module';

describe('AppController (e2e)', () => {
  let app: INestApplication;
  let baseUrl: string;
  const activeClientIds = new Set<string>();
  const openConnections: Array<{ req: ClientRequest; res: IncomingMessage }> = [];

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
    await app.listen(0);
    baseUrl = await app.getUrl();
  });

  afterEach(async () => {
    for (const clientId of activeClientIds) {
      await request(baseUrl).post('/sse/cancel').send({ clientId });
    }

    activeClientIds.clear();

    for (const connection of openConnections) {
      connection.req.destroy();
      connection.res.destroy();
    }

    openConnections.length = 0;
  });

  afterAll(async () => {
    await app.close();
  });

  async function openSse(
    path: string,
    clientId: string,
    headers: Record<string, string> = {},
  ): Promise<IncomingMessage> {
    const url = new URL(path, baseUrl);

    const response = await new Promise<IncomingMessage>((resolve, reject) => {
      const req = httpRequest(url, { method: 'GET', headers }, (res) => {
        openConnections.push({ req, res });
        resolve(res);
      });

      req.on('error', reject);
      req.end();
    });

    activeClientIds.add(clientId);
    return response;
  }

  async function readFirstChunk(response: IncomingMessage, timeoutMs = 1_500): Promise<string> {
    return new Promise<string>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timed out waiting for SSE payload'));
      }, timeoutMs);

      const onData = (chunk: Buffer | string) => {
        cleanup();
        resolve(chunk.toString());
      };

      const onError = (error: Error) => {
        cleanup();
        reject(error);
      };

      const cleanup = () => {
        clearTimeout(timer);
        response.off('data', onData);
        response.off('error', onError);
      };

      response.on('data', onData);
      response.on('error', onError);
    });
  }

  it('/ (GET)', () => {
    return request(app.getHttpServer())
      .get('/')
      .expect(200)
      .expect({
        service: 'nest-sse package is running',
        connections: 0,
      });
  });

  it('opens a public stream and allows cancellation via /sse/cancel', async () => {
    const clientId = `public-${Date.now()}`;
    const response = await openSse(`/demo/connect/public?clientId=${clientId}`, clientId);

    expect(response.statusCode).toBe(200);
    expect(String(response.headers['content-type'] ?? '')).toContain('text/event-stream');

    const firstChunk = await readFirstChunk(response);
    expect(firstChunk).toContain('event: connection.ready');

    await request(baseUrl).post('/sse/cancel').send({ clientId }).expect(204);
    activeClientIds.delete(clientId);

    const diagnostics = await request(baseUrl).get('/sse/connections').expect(200);
    expect(diagnostics.body.items).toEqual([]);
  });

  it('rejects authenticated stream open when x-user-id header is missing', async () => {
    await request(baseUrl)
      .get('/demo/connect/auth?clientId=missing-user')
      .expect(400);
  });

  it('rejects subscribe to authenticated channels from a public connection', async () => {
    const clientId = `pub-${Date.now()}`;
    await openSse(`/demo/connect/public?clientId=${clientId}`, clientId);

    await request(baseUrl)
      .post('/demo/subscribe')
      .send({
        clientId,
        channel: 'demo.private.42',
      })
      .expect(403);
  });

  it('allows authenticated subscribe/unsubscribe on matching private channel', async () => {
    const clientId = `auth-${Date.now()}`;
    await openSse(`/demo/connect/auth?clientId=${clientId}`, clientId, {
      'x-user-id': '42',
    });

    await request(baseUrl)
      .post('/demo/subscribe')
      .send({
        clientId,
        channel: 'demo.private.42',
        metadata: { userId: '42' },
      })
      .expect(200)
      .expect({ ok: true });

    await request(baseUrl)
      .post('/demo/unsubscribe')
      .send({
        clientId,
        channel: 'demo.private.42',
      })
      .expect(200)
      .expect({ ok: true });
  });

  it('applies broadcast target filters for public and authenticated audiences', async () => {
    const publicClientId = `pub-target-${Date.now()}`;
    const authClientId = `auth-target-${Date.now()}`;

    await openSse(`/demo/connect/public?clientId=${publicClientId}`, publicClientId);
    await openSse(`/demo/connect/auth?clientId=${authClientId}`, authClientId, {
      'x-user-id': '100',
    });

    await request(baseUrl)
      .post('/demo/subscribe')
      .send({ clientId: publicClientId, channel: 'demo.public' })
      .expect(200);

    await request(baseUrl)
      .post('/demo/subscribe')
      .send({ clientId: authClientId, channel: 'demo.public' })
      .expect(200);

    const authOnly = await request(baseUrl)
      .post('/demo/batch')
      .send({
        channel: 'demo.public',
        target: 'authenticated',
        count: 1,
        event: 'demo.targeted.auth',
      })
      .expect(201);

    const publicOnly = await request(baseUrl)
      .post('/demo/batch')
      .send({
        channel: 'demo.public',
        target: 'public',
        count: 1,
        event: 'demo.targeted.public',
      })
      .expect(201);

    const all = await request(baseUrl)
      .post('/demo/batch')
      .send({
        channel: 'demo.public',
        target: 'all',
        count: 1,
        event: 'demo.targeted.all',
      })
      .expect(201);

    expect(authOnly.body.delivered).toBe(1);
    expect(publicOnly.body.delivered).toBe(1);
    expect(all.body.delivered).toBe(2);
  });
});
