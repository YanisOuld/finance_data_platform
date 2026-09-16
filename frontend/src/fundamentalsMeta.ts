// Readable labels + curated "Key financials" set for SEC XBRL concept tags.
// Each metric lists candidate tags in priority order (companies tag the same
// line differently) and we use whichever the company reports.

export type MetricKind = "money" | "perShare" | "shares";

export interface KeyMetric {
  label: string;
  kind: MetricKind;
  concepts: string[];
}

export const KEY_METRICS: KeyMetric[] = [
  { label: "Revenue", kind: "money", concepts: [
    "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
    "us-gaap:Revenues",
    "us-gaap:SalesRevenueNet",
  ] },
  { label: "Cost of revenue", kind: "money", concepts: [
    "us-gaap:CostOfGoodsAndServicesSold",
    "us-gaap:CostOfRevenue",
  ] },
  { label: "Gross profit", kind: "money", concepts: ["us-gaap:GrossProfit"] },
  { label: "Operating income", kind: "money", concepts: ["us-gaap:OperatingIncomeLoss"] },
  { label: "Net income", kind: "money", concepts: ["us-gaap:NetIncomeLoss"] },
  { label: "EPS (diluted)", kind: "perShare", concepts: ["us-gaap:EarningsPerShareDiluted"] },
  { label: "EPS (basic)", kind: "perShare", concepts: ["us-gaap:EarningsPerShareBasic"] },
  { label: "Total assets", kind: "money", concepts: ["us-gaap:Assets"] },
  { label: "Total liabilities", kind: "money", concepts: ["us-gaap:Liabilities"] },
  { label: "Shareholders' equity", kind: "money", concepts: [
    "us-gaap:StockholdersEquity",
    "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
  ] },
  { label: "Cash & equivalents", kind: "money", concepts: [
    "us-gaap:CashAndCashEquivalentsAtCarryingValue",
  ] },
  { label: "Operating cash flow", kind: "money", concepts: [
    "us-gaap:NetCashProvidedByUsedInOperatingActivities",
  ] },
  { label: "Shares outstanding", kind: "shares", concepts: [
    "dei:EntityCommonStockSharesOutstanding",
    "us-gaap:CommonStockSharesOutstanding",
  ] },
];

export const KEY_METRIC_CONCEPTS: string[] = [...new Set(KEY_METRICS.flatMap((m) => m.concepts))];

// "us-gaap:RevenueFromContract..." -> "Revenue From Contract...".
export function prettyConcept(concept: string | null | undefined): string {
  if (!concept) return "";
  const local = concept.includes(":") ? concept.split(":").slice(1).join(":") : concept;
  return local.replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2");
}

// Compact large monetary/share values: 109_417_000_000 -> "109.42B".
export function fmtCompact(v: number | string | null | undefined, kind: MetricKind): string {
  if (v === null || v === undefined) return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return "";
  if (kind === "perShare") return n.toFixed(2);
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(2)}K`;
  return `${sign}${abs.toFixed(kind === "shares" ? 0 : 2)}`;
}
