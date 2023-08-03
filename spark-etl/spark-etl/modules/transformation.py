

class Transformation:
    def __init(self):
        pass 

    def count_by_country(self, survey_df):
        filtered_survey_df = survey_df \
            .where("Age < 40") \
            .select("Age","Gender","Country","state") \
            .groupBy("Country") \
            .count()
        return filtered_survey_df
     