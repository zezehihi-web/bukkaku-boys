type ResultCount = {
  label: string;
  count: number;
  color: string;
};

type ResultDonutProps = {
  data: ResultCount[];
};

export function ResultDonut({ data }: ResultDonutProps) {
  const total = data.reduce((sum, d) => sum + d.count, 0);
  if (total === 0) return null;

  // Build conic-gradient segments
  let accumulated = 0;
  const segments = data
    .filter((d) => d.count > 0)
    .map((d) => {
      const start = accumulated;
      const end = accumulated + (d.count / total) * 360;
      accumulated = end;
      return `${d.color} ${start}deg ${end}deg`;
    });

  const gradient = `conic-gradient(${segments.join(", ")})`;

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-4">
        結果分布
      </h3>
      <div className="flex items-center gap-6">
        <div
          className="w-20 h-20 rounded-full shrink-0"
          style={{
            background: gradient,
            mask: "radial-gradient(circle at center, transparent 55%, black 56%)",
            WebkitMask: "radial-gradient(circle at center, transparent 55%, black 56%)",
          }}
        />
        <div className="space-y-1.5 flex-1 min-w-0">
          {data
            .filter((d) => d.count > 0)
            .map((d) => (
              <div key={d.label} className="flex items-center gap-2 text-xs">
                <span
                  className="w-2.5 h-2.5 rounded-full shrink-0"
                  style={{ backgroundColor: d.color }}
                />
                <span className="text-gray-600 truncate">{d.label}</span>
                <span className="text-gray-400 tabular-nums ml-auto">
                  {d.count}件 ({Math.round((d.count / total) * 100)}%)
                </span>
              </div>
            ))}
        </div>
      </div>
    </div>
  );
}
