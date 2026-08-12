import type { FastifyInstance } from 'fastify';

/** Health check route — no auth required */
export async function healthRoutes(app: FastifyInstance): Promise<void> {
    app.get('/health', async (_req, reply) => {
        return reply.send({
            status: 'ok',
            service: 'analytics-engine',
            timestamp: new Date().toISOString(),
        });
    });
}
