//! Exchange-agnostic aggregated order book.
//!
//! Prices and quantities are fixed-point integers (see [`crate::fixed_point`]).
//! A level exists only while its quantity is positive. Exchanges that publish
//! quantity *changes* (Kalshi's `delta_fp`) are applied with
//! [`OrderBook::apply_delta`], which returns the level's new *absolute*
//! quantity -- the value the output schema's `PriceLevelUpdate.quantity`
//! expects.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookError {
    /// The delta would leave a level with negative quantity, which means our
    /// view of the book has diverged from the exchange's.
    NegativeQuantity {
        side: Side,
        price: i64,
        current: i64,
        delta: i64,
    },
    /// A snapshot level carried a negative quantity.
    NegativeSnapshotLevel {
        side: Side,
        price: i64,
        quantity: i64,
    },
    /// A snapshot listed the same price twice on one side.
    DuplicateSnapshotLevel {
        side: Side,
        price: i64,
    },
    Overflow {
        side: Side,
        price: i64,
    },
}

impl std::fmt::Display for BookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeQuantity {
                side,
                price,
                current,
                delta,
            } => write!(
                f,
                "delta {delta} on {side} {price} would make quantity {current} negative"
            ),
            Self::NegativeSnapshotLevel {
                side,
                price,
                quantity,
            } => write!(
                f,
                "snapshot level {side} {price} has negative quantity {quantity}"
            ),
            Self::DuplicateSnapshotLevel { side, price } => {
                write!(f, "snapshot lists {side} {price} more than once")
            }
            Self::Overflow { side, price } => write!(f, "quantity overflow on {side} {price}"),
        }
    }
}

impl std::error::Error for BookError {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl OrderBook {
    /// Builds a book from absolute snapshot levels. Zero-quantity levels are
    /// dropped; negative or duplicated levels are rejected.
    pub fn from_levels(
        bids: impl IntoIterator<Item = (i64, i64)>,
        asks: impl IntoIterator<Item = (i64, i64)>,
    ) -> Result<Self, BookError> {
        let mut book = Self::default();
        for (side, levels) in [
            (Side::Buy, bids.into_iter().collect::<Vec<_>>()),
            (Side::Sell, asks.into_iter().collect::<Vec<_>>()),
        ] {
            let map = book.side_mut(side);
            for (price, quantity) in levels {
                if quantity < 0 {
                    return Err(BookError::NegativeSnapshotLevel {
                        side,
                        price,
                        quantity,
                    });
                }
                if map.contains_key(&price) {
                    return Err(BookError::DuplicateSnapshotLevel { side, price });
                }
                if quantity > 0 {
                    map.insert(price, quantity);
                }
            }
        }
        Ok(book)
    }

    /// Applies a quantity change and returns the level's new absolute
    /// quantity (0 means the level was removed). On error the book is left
    /// unchanged.
    pub fn apply_delta(&mut self, side: Side, price: i64, delta: i64) -> Result<i64, BookError> {
        let map = self.side_mut(side);
        let current = map.get(&price).copied().unwrap_or(0);
        let updated = current
            .checked_add(delta)
            .ok_or(BookError::Overflow { side, price })?;
        if updated < 0 {
            return Err(BookError::NegativeQuantity {
                side,
                price,
                current,
                delta,
            });
        }
        if updated == 0 {
            map.remove(&price);
        } else {
            map.insert(price, updated);
        }
        Ok(updated)
    }

    pub fn quantity(&self, side: Side, price: i64) -> i64 {
        self.side(side).get(&price).copied().unwrap_or(0)
    }

    /// Bids from best (highest) to worst.
    pub fn bids(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.bids.iter().rev().map(|(&p, &q)| (p, q))
    }

    /// Asks from best (lowest) to worst.
    pub fn asks(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.asks.iter().map(|(&p, &q)| (p, q))
    }

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }

    /// True if the best bid is at or above the best ask. A healthy exchange
    /// book is never crossed, so this is a cheap consistency check.
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(b), Some(a)) if b >= a)
    }

    pub fn level_count(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    pub fn to_snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids: self
                .bids()
                .map(|(price_level, quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks()
                .map(|(price_level, quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }

    fn side(&self, side: Side) -> &BTreeMap<i64, i64> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> OrderBook {
        OrderBook::from_levels([(400_000, 10), (420_000, 5)], [(600_000, 7)]).unwrap()
    }

    #[test]
    fn snapshot_levels_are_absolute_and_sorted_best_first() {
        let book = book();
        assert_eq!(
            book.bids().collect::<Vec<_>>(),
            [(420_000, 5), (400_000, 10)]
        );
        assert_eq!(book.asks().collect::<Vec<_>>(), [(600_000, 7)]);
        assert_eq!(book.best_bid(), Some(420_000));
        assert_eq!(book.best_ask(), Some(600_000));
        assert!(!book.is_crossed());
    }

    #[test]
    fn snapshot_drops_zero_levels_and_rejects_bad_ones() {
        let book = OrderBook::from_levels([(1, 0)], [(2, 3)]).unwrap();
        assert_eq!(book.level_count(), 1);
        assert!(matches!(
            OrderBook::from_levels([(1, -1)], []),
            Err(BookError::NegativeSnapshotLevel { .. })
        ));
        assert!(matches!(
            OrderBook::from_levels([], [(1, 1), (1, 2)]),
            Err(BookError::DuplicateSnapshotLevel {
                side: Side::Sell,
                price: 1
            })
        ));
    }

    #[test]
    fn deltas_return_absolute_quantity() {
        let mut book = book();
        assert_eq!(book.apply_delta(Side::Buy, 400_000, 3), Ok(13));
        assert_eq!(book.apply_delta(Side::Buy, 400_000, -4), Ok(9));
        assert_eq!(book.quantity(Side::Buy, 400_000), 9);
        // New level.
        assert_eq!(book.apply_delta(Side::Sell, 650_000, 2), Ok(2));
        assert_eq!(
            book.asks().collect::<Vec<_>>(),
            [(600_000, 7), (650_000, 2)]
        );
    }

    #[test]
    fn delta_to_zero_removes_level() {
        let mut book = book();
        assert_eq!(book.apply_delta(Side::Sell, 600_000, -7), Ok(0));
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.level_count(), 2);
    }

    #[test]
    fn negative_result_is_rejected_without_mutation() {
        let mut book = book();
        let before = book.clone();
        assert_eq!(
            book.apply_delta(Side::Buy, 420_000, -6),
            Err(BookError::NegativeQuantity {
                side: Side::Buy,
                price: 420_000,
                current: 5,
                delta: -6
            })
        );
        assert_eq!(
            book.apply_delta(Side::Sell, 1, -1),
            Err(BookError::NegativeQuantity {
                side: Side::Sell,
                price: 1,
                current: 0,
                delta: -1
            })
        );
        assert_eq!(book, before);
    }

    #[test]
    fn overflow_is_rejected() {
        let mut book = OrderBook::from_levels([(1, i64::MAX)], []).unwrap();
        assert!(matches!(
            book.apply_delta(Side::Buy, 1, 1),
            Err(BookError::Overflow { .. })
        ));
    }

    #[test]
    fn crossed_detection() {
        let mut book = book();
        assert_eq!(book.apply_delta(Side::Buy, 600_000, 1), Ok(1));
        assert!(book.is_crossed());
    }

    #[test]
    fn to_snapshot_matches_book() {
        let ts = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
        let snap = book().to_snapshot("SYM", ts, ts);
        assert_eq!(snap.symbol, "SYM");
        assert_eq!(
            snap.bids
                .iter()
                .map(|l| (l.price_level, l.quantity))
                .collect::<Vec<_>>(),
            [(420_000, 5), (400_000, 10)]
        );
        assert_eq!(snap.asks.len(), 1);
    }
}
