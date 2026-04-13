# Domain Glossary
Container: CS2 tradable item (Case/Capsule). Root aggregate.
BasePrice: Weapon cases = base_cost - 1200. Capsules = base_cost.
KeyPrice: ~1200 (config.key_price).
Spread: Gap between lowest_price and price. Max 25% for flip.
NetUnit: _net(sell_target) - buy_target. Must be > 0.
CAGRGross: (current / oldest)^(1/years) - 1.
CAGRNet: (current_net / oldest)^(1/years) - 1. current_net = current / 1.15 - 5.