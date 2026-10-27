"use client";

import type { HTMLAttributes } from "react";
import { ArrowDown, ArrowUp, GripVertical } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useI18n } from "@/lib/i18n/provider";

interface OrderControlsProps {
  position: number;
  label: string;
  disabled: boolean;
  first: boolean;
  last: boolean;
  handleProps: HTMLAttributes<HTMLElement>;
  onMove: (offset: number) => void;
}

export function OrderControls({
  position, label, disabled, first, last, handleProps, onMove,
}: OrderControlsProps) {
  const { t } = useI18n();
  return (
    <div className="flex items-center gap-1">
      <Button
        {...handleProps}
        type="button"
        variant="ghost"
        size="icon-sm"
        disabled={disabled || (first && last)}
        aria-label={label}
        title={t("拖动排序，或使用方向键上下移动")}
        className="cursor-grab active:cursor-grabbing"
        onKeyDown={(event) => {
          if (event.key === "ArrowUp" || event.key === "ArrowDown") {
            event.preventDefault();
            onMove(event.key === "ArrowUp" ? -1 : 1);
          }
        }}
      >
        <GripVertical />
      </Button>
      <span className="min-w-6 text-center font-mono text-xs tabular-nums">{position}</span>
      <div className="flex flex-col">
        <Button
          type="button" variant="ghost" size="icon-xs"
          aria-label={t("上移：{label}", { label })}
          disabled={disabled || first} onClick={() => onMove(-1)}
        ><ArrowUp /></Button>
        <Button
          type="button" variant="ghost" size="icon-xs"
          aria-label={t("下移：{label}", { label })}
          disabled={disabled || last} onClick={() => onMove(1)}
        ><ArrowDown /></Button>
      </div>
    </div>
  );
}
