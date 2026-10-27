"use client";

import { useRef, useState, type DragEvent } from "react";

export function useListReorder(
  keys: readonly string[],
  disabled: boolean,
  onMove: (source: string, target: string) => void,
) {
  const source = useRef<string | null>(null);
  const [dropTarget, setDropTarget] = useState<string | null>(null);
  const clear = () => {
    source.current = null;
    setDropTarget(null);
  };

  return {
    rowProps: (key: string) => ({
      "data-drop-target": dropTarget === key || undefined,
      onDragOver: (event: DragEvent<HTMLElement>) => {
        if (disabled || !source.current || !keys.includes(source.current)) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = "move";
        setDropTarget(key);
      },
      onDrop: (event: DragEvent<HTMLElement>) => {
        const from = source.current;
        if (!disabled && from && keys.includes(from) && keys.includes(key)) {
          event.preventDefault();
          onMove(from, key);
        }
        clear();
      },
    }),
    handleProps: (key: string) => ({
      draggable: !disabled && keys.length > 1,
      onDragStart: (event: DragEvent<HTMLElement>) => {
        if (disabled || keys.length < 2) {
          event.preventDefault();
          return;
        }
        source.current = key;
        event.dataTransfer.effectAllowed = "move";
        event.dataTransfer.setData("text/plain", key);
      },
      onDragEnd: clear,
    }),
    moveBy: (key: string, offset: number) => {
      if (disabled) return;
      const index = keys.indexOf(key);
      const target = keys[index + offset];
      if (index >= 0 && target) onMove(key, target);
    },
  };
}
