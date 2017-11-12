class Ranker(object):
    """
    Ranks relevant results using a custom function before returning.

    The current implementation uses a simple implementation which just involves sorting
    the results on the basis of view counts, with higher view counts appearing higher on
    the final results.
    """

    @classmethod
    def rank(cls, unranked_results):
        """
            Rank results using a custom metric.

            Args:
                unranked_results(list(tuple)): List of unranked tuples of the form
                    (article_id, view_count)

            Returns:
                list(str): A ranked list of article ids.
        """
        # Sort results in descending order on the basis of View Counts.
        sorted_results = list(sorted(unranked_results, key=lambda x: x[1], reverse=True))

        ranked_results = [tup[0] for tup in sorted_results]

        return ranked_results
