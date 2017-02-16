# reference for this code: http://stackoverflow.com/questions/27950432/python-urljoin-not-removing-superflous-dots/40536710#40536710
from urlparse import urlsplit, urlunsplit


def clean_up_dots_in_url(url):
    parts = list(urlsplit(url))
    segments = parts[2].split('/')
    segments = [segment + '/' for segment in segments[:-1]] + [segments[-1]]
    resolved = []
    for segment in segments:
        if segment in ('../', '..'):
            if resolved[1:]:
                resolved.pop()
        elif segment not in ('./', '.'):
            resolved.append(segment)
    parts[2] = ''.join(resolved)
    return urlunsplit(parts)