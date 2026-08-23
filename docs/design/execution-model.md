# Execution Model

## Fundamental invariant

```text
Target Position != Order != Fill
```

A strategy may desire a position. That target position produces an order intent by reconciliation with the actual current portfolio. A submitted or requested quantity may not fully execute. Only actual fills are allowed to change the portfolio.

```text
Market Snapshot + Forecast
        ↓
Strategy
        ↓
Target Position
        ↓
Current Portfolio
        ↓
Order Intent
        ↓
Execution
        ↓
Fill(s)
        ↓
Updated Portfolio
```

## Additional invariants

- Portfolio holdings change only because of fills or settlement.
- Cash changes only because of actual fills, fees, deposits or withdrawals, or settlement.
- The simulator must never invent liquidity.
- Historical execution should use executable bid and ask information rather than only a displayed market probability.
- Partial fills are first-class state.
- Desired quantity, submitted quantity, filled quantity, and remaining quantity must remain distinguishable.
- Selling more shares than the portfolio owns must not occur unless a future explicitly documented instrument model permits it.
