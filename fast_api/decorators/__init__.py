def fence(fence_text: str = "+"):
    def add_fence(func):
        def wrapper(text: str):
            print(fence_text * len(text))
            func(text)
            print(fence_text * len(text))
        return wrapper
    return add_fence


@fence("+")
def log(text: str):
    print(text)

