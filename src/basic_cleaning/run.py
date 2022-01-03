#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    ######################
    # YOUR CODE HERE     #
    ######################

    local_path = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(local_path)
    original_size = df.size

    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price

    logger.info(f"Using min_price {min_price} and max_price {max_price}")

    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info(f"Removed {original_size - df.size} outliers")

    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Fully qualified name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the artifact to create",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the created artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Miniumum price allowed in the dataset, used to filter out invalid data",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price allowed in the dataset, used to filter out invalid data",
        required=True
    )


    args = parser.parse_args()

    go(args)
