import { App, Fn, Arn, CfnParameter, Duration } from "@aws-cdk/core";
import { Runtime } from "@aws-cdk/aws-lambda";
import { Schedule } from "@aws-cdk/aws-events";
import { PolicyStatement } from "@aws-cdk/aws-iam";
import type { GuStackProps } from "@guardian/cdk/lib/constructs/core";
import { GuStack } from "@guardian/cdk/lib/constructs/core";
import { GuScheduledLambda } from "@guardian/cdk/lib/patterns/scheduled-lambda";

class UKCoronavirusDataAlertsStack extends GuStack {
    constructor(scope: App, id: string, emailType: string, props: GuStackProps) {
        super(scope, id, props);

        const app = `${emailType}-uk-coronavirus-data-alerts`;

        // Pass these as a parameter to avoid putting email addresses in the open
        const notifyEmailAddresses = new CfnParameter(this, 'NotifyEmailAddresses', {
            description: 'Comma-separated list of email addresses to notify'
        });

        const lambda = new GuScheduledLambda(this, 'Lambda', {
            app,
            runtime: Runtime.PYTHON_3_8,
            handler: 'main.lambda_handler',
            fileName: `${app}.zip`,
            environment: {
                'NOTIFY_EMAIL_ADDRESSES': notifyEmailAddresses.valueAsString,
                'EMAIL_TYPE': emailType
            },
            monitoringConfiguration: {
                toleratedErrorPercentage: 0,
                snsTopicName: Fn.importValue('AlertSNSTopic-PROD')
            },
            rules: [
                {
                    schedule: Schedule.expression('cron(0 8 ? * MON-FRI *)'),
                    description: 'Check UK coronavirus data every weekday at 8AM'
                }
            ],
            timeout: Duration.minutes(2)
        });

        lambda.addToRolePolicy(new PolicyStatement({
            actions: ['s3:GetObject', 's3:PutObject'],
            resources: ['arn:aws:s3:::investigations-data-dev/*']
        }));

        lambda.addToRolePolicy(new PolicyStatement({
            actions: ['ses:SendEmail'],
            resources: [Arn.format({
                service: 'ses',
                resource: 'identity',
                resourceName: 'investigations.and.reporting@theguardian.com'
            }, this)]
        }));
    }
}

new UKCoronavirusDataAlertsStack(new App(), 'UKCoronavirusDataAlerts', 'VERIFIED', {
    stack: 'pfi-structured'
});

new UKCoronavirusDataAlertsStack(new App(), 'UKCoronavirusDataAlerts', 'UNVERIFIED', {
    stack: 'pfi-structured'
});