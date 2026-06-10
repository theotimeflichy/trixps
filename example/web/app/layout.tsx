import "./globals.css";
import type { Metadata } from "next";
import type { ReactNode } from "react";

export const metadata: Metadata = {
  title: "TrixPS Tic-Tac-Toe",
  description: "Online tic-tac-toe played over the TrixPS pub-sub",
};

/**
 * RootLayout wraps every page with the institutional header.
 *
 * @param children the page content
 */
export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header className="header">
          <span className="wordmark">TrixPS Tic-Tac-Toe</span>
        </header>
        {children}
      </body>
    </html>
  );
}
