type StatCardProps = {
  label: string;
  value: number;
  color?: string;
  bg?: string;
  urgent?: boolean;
};

export function StatCard({ label, value, color = "text-gray-900", bg = "bg-white", urgent }: StatCardProps) {
  return (
    <div
      className={`${bg} rounded-xl shadow-sm border border-gray-200 p-5 transition-shadow hover:shadow-md ${
        urgent ? "ring-2 ring-orange-200" : ""
      }`}
    >
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">{label}</p>
      <p className={`text-3xl font-bold mt-2 tabular-nums ${color}`}>{value}</p>
    </div>
  );
}
