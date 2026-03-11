type DailyCount = {
  date: string;
  count: number;
};

type MiniBarChartProps = {
  data: DailyCount[];
};

export function MiniBarChart({ data }: MiniBarChartProps) {
  const maxCount = Math.max(...data.map((d) => d.count), 1);

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-4">
        過去7日間の確認件数
      </h3>
      <div className="flex items-end gap-2 h-24">
        {data.map((d) => {
          const height = (d.count / maxCount) * 100;
          const dayLabel = new Date(d.date).toLocaleDateString("ja-JP", { weekday: "short" });
          return (
            <div key={d.date} className="flex-1 flex flex-col items-center gap-1">
              <span className="text-xs text-gray-500 tabular-nums">{d.count}</span>
              <div className="w-full relative" style={{ height: "80px" }}>
                <div
                  className="absolute bottom-0 w-full bg-blue-500 rounded-t transition-all duration-500 hover:bg-blue-600"
                  style={{ height: `${Math.max(height, 4)}%` }}
                />
              </div>
              <span className="text-[10px] text-gray-400">{dayLabel}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
