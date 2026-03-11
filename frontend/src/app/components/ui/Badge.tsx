const VARIANTS = {
  success: "bg-green-100 text-green-800",
  warning: "bg-yellow-100 text-yellow-800",
  error: "bg-red-100 text-red-800",
  info: "bg-blue-100 text-blue-800",
  neutral: "bg-gray-100 text-gray-700",
  orange: "bg-orange-100 text-orange-800",
} as const;

type BadgeProps = {
  variant: keyof typeof VARIANTS;
  children: React.ReactNode;
  className?: string;
};

export function Badge({ variant, children, className }: BadgeProps) {
  return (
    <span
      className={`text-xs px-2.5 py-1 rounded-full font-medium inline-flex items-center gap-1.5 ${VARIANTS[variant]} ${className ?? ""}`}
    >
      {children}
    </span>
  );
}
