export type SystemOverviewFileInfo = {
  key: string;
  label: string;
  path: string;
  exists: boolean;
  fileCount: number;
  sizeBytes: number;
  sizeLabel: string;
  modifiedAt?: string | null;
};

export type SystemOverviewPriority = {
  priority: string;
  label: string;
  notifyRule: string;
  archiveRule: string;
  status: string;
};

export type SystemOverviewModule = {
  key: string;
  name: string;
  priority: string;
  status: string;
  notifyRule: string;
  archivePath: string;
  detail: string;
};

export type SystemOverviewRecommendation = {
  level: string;
  title: string;
  description: string;
};

export type SystemOverviewServiceStatus = {
  key: string;
  name: string;
  status: string;
  detail: string;
  pid?: number | null;
  updatedAt?: string | null;
};

export type SystemOverviewAlert = {
  level: string;
  title: string;
  description: string;
};

export type SystemOverview = {
  generatedAt: string;
  scheduler: {
    scheduleEnabled?: boolean;
    scheduleTime?: string;
    nightlyMarketOutlookEnabled?: boolean;
    nightlyMarketOutlookTime?: string;
    stockIntradayReminderEnabled?: boolean;
    lockPath?: string;
    lockPid?: number | null;
    lockAlive?: boolean;
    launchAgentLabel?: string;
    launchAgentAlive?: boolean;
    launchAgentPid?: number | null;
  };
  services?: SystemOverviewServiceStatus[];
  dataWarehouse?: {
    database?: Record<string, unknown>;
    reports?: Record<string, unknown>;
    healthArchive?: Record<string, unknown>;
    disk?: Record<string, unknown>;
  };
  alerts?: SystemOverviewAlert[];
  priorities: SystemOverviewPriority[];
  modules: SystemOverviewModule[];
  files: SystemOverviewFileInfo[];
  recommendations: SystemOverviewRecommendation[];
};
