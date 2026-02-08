import type { Metadata } from 'next';
import { Inter, JetBrains_Mono } from 'next/font/google';

import { Nav } from './components/nav';
import { Footer } from './components/footer';
import './globals.css';

const inter = Inter({
  subsets: ['latin'],
  weight: ['400', '500', '600', '700'],
  variable: '--font-sans',
  display: 'swap',
});

const jetbrainsMono = JetBrains_Mono({
  subsets: ['latin'],
  weight: ['400', '500'],
  variable: '--font-mono',
  display: 'swap',
});

export const metadata: Metadata = {
  title: 'holywell — SQL Formatter',
  description:
    'Zero-config SQL formatter with river alignment. Format SQL in your browser.',
  openGraph: {
    title: 'holywell — SQL Formatter',
    description:
      'Zero-config SQL formatter with river alignment. Format SQL in your browser.',
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'holywell — SQL Formatter',
    description:
      'Zero-config SQL formatter with river alignment. Format SQL in your browser.',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className={`${inter.variable} ${jetbrainsMono.variable}`}>
      <body className={`${inter.className} min-h-screen flex flex-col`}>
        <Nav />
        <main className="flex-1">{children}</main>
        <Footer />
      </body>
    </html>
  );
}
