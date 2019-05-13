

# For instance, Alpha#101 in Appendix A is a
# delay-1 momentum alpha: if the stock runs up intraday (i.e., close > open and high > low), the
# next day one takes a long position in the stock.

# alpha 101 = ((close - open) / ((high - low) + .001))

# should strategy also generate signals using a Signal object?
# maybe signal is the indicator on which long and short rules are based

class Strategy:
    def __init__(self, price_data, signal):
        self.price_data = price_data
        self.signal = signal

    def long(self):
        pass

    def short(self):
        pass

