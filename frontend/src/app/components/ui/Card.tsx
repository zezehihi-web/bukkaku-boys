type CardProps = {
  title?: string;
  children: React.ReactNode;
  className?: string;
  padding?: "none" | "normal";
};

export function Card({ title, children, className, padding = "normal" }: CardProps) {
  return (
    <section
      className={`bg-white rounded-xl shadow-sm border border-gray-200 ${
        padding === "normal" ? "p-6" : ""
      } ${className ?? ""}`}
    >
      {title && (
        <h2 className="text-lg font-semibold text-gray-900 mb-4">{title}</h2>
      )}
      {children}
    </section>
  );
}
