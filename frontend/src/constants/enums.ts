export const Roles={admin:'运营研究负责人',analyst:'排队分析员',viewer:'窗口观察员'} as const;
export const RunStatus={queued:'排队计算',running:'仿真中',completed:'已完成',failed:'失败'} as const;
export const RunEvent={claimed:'接手执行',taken_over:'自动接管续算',replication_saved:'保存独立重复',completed:'发布完成版本',failed:'执行失败',released:'让出接管权'} as const;
export const ReviewStatus={submitted:'待审阅',approved:'已批准',rejected:'退回补充'} as const;
