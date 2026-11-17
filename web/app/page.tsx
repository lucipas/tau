// Placeholder home page. The UI agents replace this with the social-app
// layout (home feed, left nav rail, right peer sidebar). It exists so the
// static export has a route; it renders nothing but the shell title.
export default function Home() {
  return (
    <main className="flex min-h-screen items-center justify-center">
      <h1 className="text-xl font-semibold tracking-tight">
        τ <span className="text-zinc-400">/ MESH CONSOLE</span>
      </h1>
    </main>
  );
}
