const STEPS = [
  { key: "parsing", label: "URL解析" },
  { key: "matching", label: "物件照合" },
  { key: "checking", label: "空室確認" },
] as const;

function getStepIndex(status: string): number {
  const map: Record<string, number> = {
    pending: 0,
    parsing: 0,
    matching: 1,
    awaiting_platform: 1,
    checking: 2,
    done: 3,
    not_found: 3,
    error: 3,
  };
  return map[status] ?? 0;
}

type StepIndicatorProps = {
  status: string;
};

export function StepIndicator({ status }: StepIndicatorProps) {
  const stepIndex = getStepIndex(status);

  return (
    <div className="rounded-xl p-6 bg-white border border-gray-200">
      <div className="flex items-center justify-between mb-6">
        {STEPS.map((step, i) => (
          <div key={step.key} className="flex items-center flex-1">
            <div className="flex flex-col items-center">
              <div
                className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold transition-colors duration-500 ${
                  i < stepIndex
                    ? "bg-blue-600 text-white"
                    : i === stepIndex
                      ? "bg-blue-100 text-blue-600 ring-4 ring-blue-50"
                      : "bg-gray-100 text-gray-400"
                }`}
              >
                {i < stepIndex ? (
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                  </svg>
                ) : (
                  i + 1
                )}
              </div>
              <span
                className={`mt-2 text-xs font-medium ${
                  i <= stepIndex ? "text-blue-600" : "text-gray-400"
                }`}
              >
                {step.label}
              </span>
            </div>
            {i < STEPS.length - 1 && (
              <div
                className={`flex-1 h-0.5 mx-3 transition-colors duration-500 ${
                  i < stepIndex ? "bg-blue-600" : "bg-gray-200"
                }`}
              />
            )}
          </div>
        ))}
      </div>
      <p className="text-center text-sm text-gray-500">
        空室状況を確認しています。通常30秒ほどで完了します。
      </p>
    </div>
  );
}
