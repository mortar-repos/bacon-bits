from org.apache.pig.scripting import Pig
from workflow                 import PigWorkflow

if __name__ == "__main__":
    test_workflow = PigWorkflow()

    test_workflow.add_step(
        "script_1.pig",
        "locating aardvarks",
        {
            "INPUT_PATH"  : "../example_input/1234.txt",
            "OUTPUT_PATH" : "../example_output/crossed"
        }
    ).add_step(
        "script_2.pig",
        "consulting with aardvark elders",
        {
            "INPUT_PATH"  : "../example_output/crossed",
            "SHOUTPUT_PATH" : "../example_output/func_1"
        },
        checkpoint_path_param="SHOUTPUT_PATH"
    ).add_step(
        "script_3.pig",
        "finalizing free trade agreement",
        {
            "INPUT_PATH"  : "../example_output/func_1",
            "OUTPUT_PATH" : "../example_output/func_2"
        }
    )

    test_workflow.run()
