export async function register() {
  if (process.env.NEXT_RUNTIME !== 'nodejs') return

  try {
    const { tracer } = await import('dd-trace')
    const { envClient } = await import('./envClient')

    const nodeEnv = envClient.NEXT_PUBLIC_NODE_ENV || 'development'
    const isDevelopment = nodeEnv === 'development'

    tracer.init({
      service: 'latitude-llm-web',
      env: nodeEnv,
      version: envClient.NEXT_PUBLIC_RELEASE_VERSION || '1.0.0',
      logInjection: !isDevelopment,
      runtimeMetrics: true,
    })

    tracer.use('http', {
      service: 'latitude-llm-web',
    })

    tracer.use('next', {
      service: 'latitude-llm-web',
    })

    tracer.use('ioredis', {
      blocklist: ['xread'],
    })
  } catch (e) {
    console.warn('dd-trace initialization failed, continuing without APM:', e)
  }
}
