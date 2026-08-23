# Initial Architecture

The conceptual system consists of separable components:

```text
Market Data
     │
     ▼
Forecasting
     │
     ▼
Strategy / Edge Evaluation
     │
     ▼
Position Sizing + Risk
     │
     ▼
Target Position
     │
     ▼
Portfolio Reconciliation
     │
     ▼
Order Intent
     │
     ▼
Execution
     │
     ▼
Fill
     │
     ▼
Portfolio
```

The boundaries are intentional: forecasting, strategy and edge evaluation, position sizing and risk, portfolio accounting, market data, execution, and infrastructure must remain conceptually separable.

The architecture must eventually support three execution modes:

1. historical replay;
2. live shadow or paper trading; and
3. live execution.

The same high-level strategy logic should not need to be rewritten for each mode. Mode-specific market data and execution behaviour should remain behind explicit boundaries, while portfolio reconciliation continues to distinguish targets, order intents, and fills.

These are conceptual requirements only. No component or execution mode is implemented at this stage.
