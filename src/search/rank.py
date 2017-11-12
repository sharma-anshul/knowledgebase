class Ranker(object):
    """
    Ranks relevant results using a custom function before returning.

    The current implementation uses a simple implementation which just involves sorting
    the results on the basis of view counts, with higher view counts appearing higher on
    the final results.
    """

    def rank(self, unranked_results):
        """
            Rank results using a custom metric.

            Args:
                unranked_results(list(tuple)): List of unranked tuples of the form
                    (article_id, article_title, view_count)

            Returns:
                list(tuple): A list of ranked tuples of the form (article_id, article_title).
        """
        # Sort results in descending order on the basis of View Counts.
        sorted_results = sorted(unranked_results, key=lambda x: x[2], reverse=True)

        ranked_results = [tup[:2] for tup in sorted_results]

        return ranked_results
