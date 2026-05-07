import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "PJM DA Frontend",
  description: "PJM day-ahead market dashboards and model outputs.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body
        suppressHydrationWarning
        className="min-h-screen bg-[#0f1117] text-gray-100 antialiased"
      >
        {children}
      </body>
    </html>
  );
}
