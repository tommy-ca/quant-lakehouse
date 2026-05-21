from binance_datatool.universe.builder import UniverseBuilder

builder = UniverseBuilder(lake_path="./lake")
spot_universe = builder.build_top_50(trade_type="spot")
um_universe = builder.build_top_50(trade_type="um")

print("--- SPOT TOP 50 ---")
print(spot_universe)
print("\n--- UM TOP 50 ---")
print(um_universe)
