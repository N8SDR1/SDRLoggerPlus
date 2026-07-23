import { QueryClient } from '@tanstack/react-query';

// Single shared QueryClient instance. Exported so non-React code (e.g. the SignalR
// service) can invalidate caches too — not just components and hooks. Created here
// and handed to <QueryClientProvider> in main.tsx.
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60, // 1 minute
      refetchOnWindowFocus: false,
      retry: 3, // Limit retries to prevent resource exhaustion
      retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000), // Exponential backoff
    },
  },
});
