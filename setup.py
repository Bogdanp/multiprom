from setuptools import setup


with open("multiprom/__init__.py", "r") as f:
    version_marker = "__version__ = "
    for line in f:
        if line.startswith(version_marker):
            _, version = line.split(version_marker)
            version = version.strip().strip('"')
            break
    else:
        raise RuntimeError("Version marker not found.")


def parse_dependencies(filename):
    with open(filename) as reqs:
        for line in reqs:
            line = line.strip()
            if line.startswith("#"):
                continue

            elif line.startswith("-r"):
                yield from parse_dependencies(line[len("-r "):])

            else:
                yield line


dependencies = list(parse_dependencies("requirements.txt"))

extras = ()
extra_dependencies = {}
for extra in extras:
    filename = "{}.txt".format(extra)
    extra_dependencies[extra] = list(parse_dependencies(filename))


setup(
    name="multiprom",
    version=version,
    packages=["multiprom"],
    install_requires=dependencies,
    extras_require=extra_dependencies,
)
