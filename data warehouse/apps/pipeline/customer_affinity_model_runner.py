from projects.customer_affinity_model.customer_affinity_model import (
    CustomerAffinityModel,
)


def execute():
    task = CustomerAffinityModel("customer_affinity_model")
    task.run()


if __name__ == "__main__":
    execute()
