package jobs

import "strings"

func normalizeName(name string) string {
	return strings.TrimSpace(name)
}

func normalizeQueue(name string) string {
	queue := strings.TrimSpace(name)
	if queue == "" {
		return defaultQueue
	}
	return queue
}

func copyMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func copyTags(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		out = append(out, tag)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
