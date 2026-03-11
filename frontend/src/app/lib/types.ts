export type CheckItem = {
  id: number;
  property_name: string;
  status: string;
  vacancy_result: string;
  portal_source: string;
  created_at: string;
};

export type CheckStatus = {
  id: number;
  submitted_url: string;
  portal_source: string;
  property_name: string;
  property_address: string;
  property_rent: string;
  property_area: string;
  property_layout: string;
  property_build_year: string;
  atbb_matched: boolean;
  atbb_company: string;
  platform: string;
  platform_auto: boolean;
  status: string;
  vacancy_result: string;
  error_message: string;
  created_at: string;
  completed_at: string | null;
};

export type KnowledgeItem = {
  id: number;
  company_name: string;
  company_phone: string;
  platform: string;
  use_count: number;
  last_used_at: string;
};

export type PhoneTask = {
  id: number;
  check_request_id: number | null;
  company_name: string;
  company_phone: string;
  property_name: string;
  property_address: string;
  reason: string;
  status: string;
  note: string;
  created_at: string;
  completed_at: string | null;
};

export type BatchCheckResponse = {
  ids: number[];
};

export type PlatformStatus = Record<
  string,
  { configured: boolean; label: string }
>;

export type DashboardStats = {
  today_total: number;
  processing: number;
  awaiting_platform: number;
  phone_tasks_pending: number;
};
