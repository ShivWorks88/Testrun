import { createContext, useContext, useState, type ReactNode } from "react";

// The business date selected in the top bar; every screen reads it.
// null means "latest available", resolved by the API.
type Ctx = { date: string | null; setDate: (d: string | null) => void };
const BusinessDateContext = createContext<Ctx>({ date: null, setDate: () => {} });

export function BusinessDateProvider({ children }: { children: ReactNode }) {
  const [date, setDate] = useState<string | null>(null);
  return <BusinessDateContext.Provider value={{ date, setDate }}>{children}</BusinessDateContext.Provider>;
}

export const useBusinessDate = () => useContext(BusinessDateContext);
