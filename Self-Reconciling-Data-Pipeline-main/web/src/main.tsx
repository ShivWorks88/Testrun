import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./app/App";
import { BusinessDateProvider } from "./app/BusinessDate";
import "./styles/tokens.css";

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false } } });

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BusinessDateProvider>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </BusinessDateProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
