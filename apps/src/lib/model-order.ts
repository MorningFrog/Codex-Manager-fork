/** Reorder only visible slots, retaining hidden and filtered items in place. */
export function moveVisibleItemOrder(
  allKeys: readonly string[],
  visibleKeys: readonly string[],
  source: string,
  target: string,
): string[] {
  const unchanged = [...allKeys];
  const all = new Set(allKeys);
  const visible = new Set(visibleKeys);
  const from = visibleKeys.indexOf(source);
  const to = visibleKeys.indexOf(target);
  if (
    source === target || from < 0 || to < 0 ||
    all.size !== allKeys.length || visible.size !== visibleKeys.length ||
    visibleKeys.some((key) => !all.has(key))
  ) return unchanged;

  const reordered = [...visibleKeys];
  reordered.splice(to, 0, reordered.splice(from, 1)[0]);
  let index = 0;
  return allKeys.map((key) => visible.has(key) ? reordered[index++] : key);
}

/** Explicit route moves establish descending priority; untouched ties stay intact. */
export function moveRouteOrder<T extends { key: string; priority: string }>(
  routes: readonly T[],
  source: string,
  target: string,
): T[] {
  const keys = routes.map((route) => route.key);
  const order = moveVisibleItemOrder(keys, keys, source, target);
  if (order.every((key, index) => key === keys[index])) return [...routes];
  const byKey = new Map(routes.map((route) => [route.key, route]));
  return order.map((key, index) => ({
    ...byKey.get(key)!,
    priority: String((order.length - index) * 10),
  }));
}
