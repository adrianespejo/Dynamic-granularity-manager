from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from data_model import Words, Result
from hecuba import config


@task(returns=int)
def WordCountTask(partition, result):
    for line, words in partition.iteritems():
        parsed_words = words.split(" ")
        for word in parsed_words:
            result[word] = result.get(word, 0) + 1

    return 1


if __name__ == "__main__":
    config.session.execute("DROP TABLE IF EXISTS my_app.result")
    words = Words("my_app.words")
    result = Result("my_app.result")

    output = []

    for partition in words.split():
        output.append(WordCountTask(partition, result))

    output = compss_wait_on(output)

    print("\n")
    for word, instances in result.iteritems():
        print("Word %s has %s instances" % (word, instances))
    print("\n")
