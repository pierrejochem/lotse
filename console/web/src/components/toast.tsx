import { createContext, useCallback, useContext, useState, type ReactNode } from 'react';
import * as Toast from '@radix-ui/react-toast';

type Item = { id: number; title: string; cli?: string; error?: boolean };
const Ctx = createContext<(t: Omit<Item, 'id'>) => void>(() => {});

/** Toasts for the outcome of writes, with the equivalent CLI command. */
export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<Item[]>([]);
  const push = useCallback((t: Omit<Item, 'id'>) => setItems((xs) => [...xs, { ...t, id: Date.now() + Math.random() }]), []);
  return (
    <Toast.Provider duration={7000}>
      <Ctx.Provider value={push}>{children}</Ctx.Provider>
      {items.map((t) => (
        <Toast.Root
          key={t.id}
          className={`toast${t.error ? ' error' : ''}`}
          type={t.error ? 'foreground' : 'background'}
          // An error says what to do next, so it stays until it is dismissed.
          duration={t.error ? Infinity : undefined}
          onOpenChange={(open) => !open && setItems((xs) => xs.filter((x) => x.id !== t.id))}
        >
          <Toast.Title className="toast-title">{t.title}</Toast.Title>
          {t.error && <Toast.Close className="btn small" aria-label="Dismiss">Dismiss</Toast.Close>}
          {t.cli && (
            <Toast.Description>
              <code>{t.cli}</code>
            </Toast.Description>
          )}
        </Toast.Root>
      ))}
      <Toast.Viewport className="toast-viewport" />
    </Toast.Provider>
  );
}

export const useToast = () => useContext(Ctx);
