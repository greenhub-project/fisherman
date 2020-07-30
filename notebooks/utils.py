def memory_usage(df, aggregate=True):
  memory = df.memory_usage(deep=True).sum() if aggregate else df.memory_usage(deep=True)
  return round(memory / 1024 ** 2, 2)