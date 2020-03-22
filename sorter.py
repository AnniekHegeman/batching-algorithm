from common import round_up

class SortNodesFactory:

    def create(type:str):
        if type == "UPS": return UpsSorter()
        if type == "PACCAR": return PaccarSorter()
        assert 0, "Wrong type creation: " + type


class UpsSorter:
    def __init__(self):
        pass

    def sort(self, nodelist: list):
        nodelist_up = sorted([x for x in nodelist if (int(round_up((x[0] - 1) / 2)) % 2) == 0], key=lambda k: k[1],
                             reverse=False)  # ascending y for nodes in aisles up
        nodelist_up = sorted(nodelist_up, key=lambda k: int(round_up((k[0] - 1) / 2)), reverse=True)
        nodelist_down = sorted([x for x in nodelist if (int(round_up((x[0] - 1) / 2)) % 2) != 0], key=lambda k: k[1],
                               reverse=True)  # descending y for nodes in aisles down
        nodelist_up = sorted(nodelist_up, key=lambda k: int(round_up((k[0] - 1) / 2)), reverse=True)
        sorted_nodelist = sorted(nodelist_up + nodelist_down, key=lambda k: int(round_up((k[0] - 1) / 2)),
                                 reverse=True)  # nodelists combined, sorted based on the aisle they are in, not X-coordinate
        return sorted_nodelist


class PaccarSorter:
    def __init__(self):
        pass

    def sort(self, nodelist: list):
        even_nodelist = sorted([x for x in nodelist if x[0] % 2 == 0], key=lambda k: k[1],
                               reverse=True)  # reverse = True/False based on direction
        odd_nodelist = sorted([x for x in nodelist if x[0] % 2 != 0], key=lambda k: k[1],
                              reverse=False)  # reverse = True/False based on direction
        nodelist = even_nodelist + odd_nodelist
        sorted_nodelist = sorted(nodelist, key=lambda k: k[0],
                                 reverse=False)  # reverse = True when picking from right to left side of warehouse
        orders_s_shape = sorted_nodelist
        return orders_s_shape