const SIZES = {
  sm: "h-4 w-4 border-2",
  md: "h-6 w-6 border-2",
  lg: "h-8 w-8 border-[3px]",
} as const;

type SpinnerProps = {
  size?: keyof typeof SIZES;
  className?: string;
};

export function Spinner({ size = "md", className }: SpinnerProps) {
  return (
    <div
      role="status"
      aria-label="読み込み中"
      className={`animate-spin rounded-full border-blue-600/30 border-t-blue-600 ${SIZES[size]} ${className ?? ""}`}
    >
      <span className="sr-only">読み込み中</span>
    </div>
  );
}
