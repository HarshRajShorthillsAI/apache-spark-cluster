from etl_pipeline import ETLPipeline
class Main:

    @classmethod
    def run_pipeline(cls):
        ETLPipeline('/data').task5()

if __name__ == "__main__":
    Main.run_pipeline()